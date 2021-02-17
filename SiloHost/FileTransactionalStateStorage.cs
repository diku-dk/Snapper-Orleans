using System;
using Orleans;
using System.IO;
using Newtonsoft.Json;
using Orleans.Runtime;
using System.Threading;
using Orleans.Transactions;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using Orleans.Transactions.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace OrleansSiloHost
{
    public class FileTransactionalStateStorageFactory : ITransactionalStateStorageFactory, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly string name;
        private readonly MyTransactionalStateOptions options;

        public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
        {
            var optionsMonitor = services.GetRequiredService<IOptionsMonitor<MyTransactionalStateOptions>>();
            return ActivatorUtilities.CreateInstance<FileTransactionalStateStorageFactory>(services, name, optionsMonitor.Get(name));
        }

        public FileTransactionalStateStorageFactory(string name, MyTransactionalStateOptions options)
        {
            this.name = name;
            this.options = options;
        }

        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainActivationContext context) where TState : class, new()
        {
            var str = context.GrainIdentity.ToString();
            var strs = str.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var partitionKey = strs[strs.Length - 1];    // use grainID as partitionKey
            return ActivatorUtilities.CreateInstance<FileTransactionalStateStorage<TState>>(context.ActivationServices, partitionKey);
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<FileTransactionalStateStorageFactory>(name), options.InitStage, Init);
        }

        private Task Init(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    public class FileTransactionalStateStorage<TState> : ITransactionalStateStorage<TState> where TState : class, new()
    {
        private SemaphoreSlim fileLock;
        private string fileName = @"C:\Users\Administrator\Desktop\logorleans\";

        private KeyEntity key;
        private List<KeyValuePair<long, StateEntity>> states;

        public FileTransactionalStateStorage(string partition)
        {
            fileLock = new SemaphoreSlim(1);
            fileName += partition;
            var f1 = File.CreateText(fileName + "_key");
            f1.Close();
            var f2 = File.CreateText(fileName + "_state");
            f2.Close();
        }

        public async Task<TransactionalStorageLoadResponse<TState>> Load()
        {
            try
            {
                var keyTask = ReadKey();
                var statesTask = ReadStates();
                key = await keyTask.ConfigureAwait(false);
                states = await statesTask.ConfigureAwait(false);

                if (string.IsNullOrEmpty(key.ETag)) return new TransactionalStorageLoadResponse<TState>();
                else
                {
                    TState committedState;
                    if (key.CommittedSequenceId == 0) committedState = new TState();
                    else
                    {
                        if (!FindState(key.CommittedSequenceId, out var pos))
                        {
                            var error = $"Storage state corrupted: no record for committed state v{key.CommittedSequenceId}";
                            throw new InvalidOperationException(error);
                        }
                        committedState = states[pos].Value.GetState<TState>();
                    }

                    var PrepareRecordsToRecover = new List<PendingTransactionState<TState>>();
                    for (int i = 0; i < states.Count; i++)
                    {
                        var kvp = states[i];

                        // pending states for already committed transactions can be ignored
                        if (kvp.Key <= key.CommittedSequenceId)
                            continue;

                        // upon recovery, local non-committed transactions are considered aborted
                        if (kvp.Value.TransactionManager == null)
                            break;

                        var tm = JsonConvert.DeserializeObject<ParticipantId>(kvp.Value.TransactionManager);

                        PrepareRecordsToRecover.Add(new PendingTransactionState<TState>()
                        {
                            SequenceId = kvp.Key,
                            State = kvp.Value.GetState<TState>(),
                            TimeStamp = kvp.Value.TransactionTimestamp,
                            TransactionId = kvp.Value.TransactionId,
                            TransactionManager = tm
                        });
                    }

                    // clear the state strings... no longer needed, ok to GC now
                    for (int i = 0; i < states.Count; i++) states[i].Value.StateJson = null;

                    var metadata = JsonConvert.DeserializeObject<TransactionalStateMetaData>(key.Metadata);
                    return new TransactionalStorageLoadResponse<TState>(key.ETag, committedState, key.CommittedSequenceId, metadata, PrepareRecordsToRecover);
                }
            }
            catch (Exception)
            {
                throw;
            }
        }


        public async Task<string> Store(string expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<TState>> statesToPrepare, long? commitUpTo, long? abortAfter)
        {
            if (key.ETag != expectedETag) throw new ArgumentException(nameof(expectedETag), "Etag does not match");

            // first, clean up aborted records
            if (abortAfter.HasValue && states.Count != 0)
            {
                while (states.Count > 0 && states[states.Count - 1].Key > abortAfter)
                    states.RemoveAt(states.Count - 1);

            }

            // second, persist non-obsolete prepare records
            var obsoleteBefore = commitUpTo.HasValue ? commitUpTo.Value : key.CommittedSequenceId;
            if (statesToPrepare != null)
                foreach (var s in statesToPrepare)
                    if (s.SequenceId >= obsoleteBefore)
                    {
                        if (FindState(s.SequenceId, out var pos))
                        {
                            // overwrite with new pending state
                            var existing = states[pos].Value;
                            existing.TransactionId = s.TransactionId;
                            existing.TransactionTimestamp = s.TimeStamp;
                            existing.TransactionManager = JsonConvert.SerializeObject(s.TransactionManager);
                            existing.SetState(s.State);
                        }
                        else
                        {
                            var entity = StateEntity.Create(s);
                            states.Insert(pos, new KeyValuePair<long, StateEntity>(s.SequenceId, entity));
                        }
                    }

            // third, persist metadata and commit position
            key.Metadata = JsonConvert.SerializeObject(metadata);
            if (commitUpTo.HasValue && commitUpTo.Value > key.CommittedSequenceId)
            {
                key.CommittedSequenceId = commitUpTo.Value;
            }

            // fourth, remove obsolete records
            if (states.Count > 0 && states[0].Key < obsoleteBefore)
            {
                FindState(obsoleteBefore, out var pos);
                states.RemoveRange(0, pos);
            }

            // re-write KeyEntity file and StateEntity file
            try
            {
                await fileLock.WaitAsync();
                var keyfile = fileName + "_key";
                using (var f = new StreamWriter(keyfile, false))
                {
                    var str = JsonConvert.SerializeObject(key);
                    f.WriteLine(str);
                }

                var statefile = fileName + "_state";
                using (var f = new StreamWriter(statefile, false))
                {
                    foreach (var state in states)
                    {
                        var str = JsonConvert.SerializeObject(state);
                        f.WriteLine(str);
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
            }
            finally
            {
                fileLock.Release();
            }

            return key.ETag;
        }

        // find the StateEntity who's Key == sequenceId
        private bool FindState(long sequenceId, out int pos)
        {
            pos = 0;
            while (pos < states.Count)
            {
                switch (states[pos].Key.CompareTo(sequenceId))
                {
                    case 0:
                        return true;
                    case -1:
                        pos++;
                        continue;
                    case 1:
                        return false;
                }
            }
            return false;
        }

        private async Task<KeyEntity> ReadKey()
        {
            try
            {
                await fileLock.WaitAsync();
                var file = fileName + "_key";

                var str = File.ReadAllText(file);
                if (str.Length == 0) return new KeyEntity();
                return JsonConvert.DeserializeObject<KeyEntity>(str);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
            }
            finally
            {
                fileLock.Release();
            }
            return new KeyEntity();
        }

        private async Task<List<KeyValuePair<long, StateEntity>>> ReadStates()
        {
            try
            {
                await fileLock.WaitAsync();
                var file = fileName + "_state";
                var results = new List<KeyValuePair<long, StateEntity>>();
                using (var f = new StreamReader(file))
                {
                    string line;
                    var count = 0;
                    while ((line = f.ReadLine()) != null)
                    {
                        var entity = JsonConvert.DeserializeObject<StateEntity>(line);
                        results.Add(new KeyValuePair<long, StateEntity>(count++, entity));
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
            }
            finally
            {
                fileLock.Release();
            }
            return new List<KeyValuePair<long, StateEntity>>();
        }
    }
}