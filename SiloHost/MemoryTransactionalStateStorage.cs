using System;
using Orleans;
using Utilities;
using Orleans.Runtime;
using Orleans.Hosting;
using System.Threading;
using Orleans.Providers;
using Orleans.Transactions;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Options;
using Orleans.Transactions.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System.IO;
using Newtonsoft.Json;

namespace OrleansSiloHost
{
    public static class MemorySiloBuilderExtensions
    {
        public static ISiloHostBuilder AddMemoryTransactionalStateStorageAsDefault(this ISiloHostBuilder builder, Action<MemoryTransactionalStateOptions> configureOptions)
        {
            return builder.AddMemoryTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static ISiloHostBuilder AddMemoryTransactionalStateStorage(this ISiloHostBuilder builder, string name, Action<MemoryTransactionalStateOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddMemoryTransactionalStateStorage(name, ob => ob.Configure(configureOptions)));
        }

        private static IServiceCollection AddMemoryTransactionalStateStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<MemoryTransactionalStateOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<MemoryTransactionalStateOptions>(name));

            services.TryAddSingleton<ITransactionalStateStorageFactory>(sp => sp.GetServiceByName<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddSingletonNamedService<ITransactionalStateStorageFactory>(name, MemoryTransactionalStateStorageFactory.Create);
            services.AddSingletonNamedService<ILifecycleParticipant<ISiloLifecycle>>(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<ITransactionalStateStorageFactory>(n));

            return services;
        }
    }

    public class MemoryTransactionalStateOptions
    {
        public int InitStage { get; set; }
    }

    public class MemoryTransactionalStateStorageFactory : ITransactionalStateStorageFactory, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly string name;
        private readonly MemoryTransactionalStateOptions options;

        public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
        {
            var optionsMonitor = services.GetRequiredService<IOptionsMonitor<MemoryTransactionalStateOptions>>();
            return ActivatorUtilities.CreateInstance<MemoryTransactionalStateStorageFactory>(services, name, optionsMonitor.Get(name));
        }

        public MemoryTransactionalStateStorageFactory(string name, MemoryTransactionalStateOptions options)
        {
            this.name = name;
            this.options = options;
        }

        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainActivationContext context) where TState : class, new()
        {
            var str = context.GrainIdentity.ToString();
            var strs = str.Split('/', StringSplitOptions.RemoveEmptyEntries);
            var partitionKey = strs[strs.Length - 1];
            //Console.WriteLine($"file = {partitionKey}");
            return ActivatorUtilities.CreateInstance<MemoryTransactionalStateStorage<TState>>(context.ActivationServices, partitionKey);
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<MemoryTransactionalStateStorageFactory>(this.name), this.options.InitStage, Init);
        }

        private Task Init(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }

    public class MemoryTransactionalStateStorage<TState> : ITransactionalStateStorage<TState>
        where TState : class, new()
    {
        private string fileName = @"C:\Users\Administrator\Desktop\logorleans\";
        private string ETag = "default";
        private KeyEntity key;
        private List<KeyValuePair<long, StateEntity>> states;
        private ISerializer serializer;
        private readonly string partition;
        private SemaphoreSlim instanceLock;

        public MemoryTransactionalStateStorage(string partition)
        {
            fileName += partition;
            key = new KeyEntity();
            states = new List<KeyValuePair<long, StateEntity>>();
            serializer = new MsgPackSerializer();
            instanceLock = new SemaphoreSlim(1);
        }

        public async Task<TransactionalStorageLoadResponse<TState>> Load()
        {
            return new TransactionalStorageLoadResponse<TState>(ETag, new TState(), 0, new TransactionalStateMetaData(), new List<PendingTransactionState<TState>>());
            /*
            try
            {
                var keyTask = ReadKey();
                var statesTask = ReadStates();
                key = await keyTask.ConfigureAwait(false);
                states = await statesTask.ConfigureAwait(false);

                if (string.IsNullOrEmpty(key.ETag))
                {
                    if (logger.IsEnabled(LogLevel.Debug))
                        logger.LogDebug($"{partition} Loaded v0, fresh");

                    // first time load
                    return new TransactionalStorageLoadResponse<TState>();
                }
                else
                {
                    TState committedState;
                    if (this.key.CommittedSequenceId == 0)
                    {
                        committedState = new TState();
                    }
                    else
                    {
                        if (!FindState(this.key.CommittedSequenceId, out var pos))
                        {
                            var error = $"Storage state corrupted: no record for committed state v{this.key.CommittedSequenceId}";
                            logger.LogCritical($"{partition} {error}");
                            throw new InvalidOperationException(error);
                        }
                        committedState = states[pos].Value.GetState<TState>(this.jsonSettings);
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

                        ParticipantId tm = JsonConvert.DeserializeObject<ParticipantId>(kvp.Value.TransactionManager, this.jsonSettings);

                        PrepareRecordsToRecover.Add(new PendingTransactionState<TState>()
                        {
                            SequenceId = kvp.Key,
                            State = kvp.Value.GetState<TState>(this.jsonSettings),
                            TimeStamp = kvp.Value.TransactionTimestamp,
                            TransactionId = kvp.Value.TransactionId,
                            TransactionManager = tm
                        });
                    }

                    // clear the state strings... no longer needed, ok to GC now
                    for (int i = 0; i < states.Count; i++)
                    {
                        states[i].Value.StateJson = null;
                    }

                    if (logger.IsEnabled(LogLevel.Debug))
                        logger.LogDebug($"{partition} Loaded v{this.key.CommittedSequenceId} rows={string.Join(",", states.Select(s => s.Key.ToString("x16")))}");

                    TransactionalStateMetaData metadata = JsonConvert.DeserializeObject<TransactionalStateMetaData>(this.key.Metadata, this.jsonSettings);
                    return new TransactionalStorageLoadResponse<TState>(this.key.ETag, committedState, this.key.CommittedSequenceId, metadata, PrepareRecordsToRecover);
                }
            }
            catch (Exception ex)
            {
                this.logger.LogError("Transactional state load failed {Exception}.", ex);
                throw;
            }
            */
        }

        public async Task<string> Store(string expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<TState>> statesToPrepare, long? commitUpTo, long? abortAfter)
        {
            await instanceLock.WaitAsync();
            FileStream file = null;
            try
            {
                file = new FileStream(fileName, FileMode.Append, FileAccess.Write);

                if (ETag != expectedETag) throw new ArgumentException(nameof(expectedETag), "Etag does not match");

                // assemble all storage operations into a single batch
                // these operations must commit in sequence, but not necessarily atomically
                // so we can split this up if needed
                //var batchOperation = new BatchOperation(logger, key, table);

                // first, clean up aborted records
                if (abortAfter.HasValue && states.Count != 0)
                {
                    while (states.Count > 0 && states[states.Count - 1].Key > abortAfter)
                    {
                        var entity = states[states.Count - 1].Value;
                        //await batchOperation.Add(TableOperation.Delete(entity)).ConfigureAwait(false);
                        states.RemoveAt(states.Count - 1);
                    }
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
                                StateEntity existing = states[pos].Value;
                                existing.TransactionId = s.TransactionId;
                                existing.TransactionTimestamp = s.TimeStamp;
                                existing.TransactionManager = JsonConvert.SerializeObject(s.TransactionManager);
                                existing.state = JsonConvert.SerializeObject(s.State);
                                //await batchOperation.Add(TableOperation.Replace(existing)).ConfigureAwait(false);

                                var data1 = serializer.serialize(existing);
                                var sizeBytes1 = BitConverter.GetBytes(data1.Length);
                                await file.WriteAsync(sizeBytes1, 0, sizeBytes1.Length);
                                await file.WriteAsync(data1, 0, data1.Length);
                            }
                            else
                            {
                                //var entity = StateEntity.Create(this.jsonSettings, this.partition, s);
                                //await batchOperation.Add(TableOperation.Insert(entity)).ConfigureAwait(false);

                                var entity = new StateEntity();
                                entity.TransactionId = s.TransactionId;
                                entity.TransactionTimestamp = s.TimeStamp;
                                entity.TransactionManager = JsonConvert.SerializeObject(s.TransactionManager);
                                entity.state = JsonConvert.SerializeObject(s.State);

                                var data2 = serializer.serialize(entity);
                                var sizeBytes2 = BitConverter.GetBytes(data2.Length);
                                await file.WriteAsync(sizeBytes2, 0, sizeBytes2.Length);
                                await file.WriteAsync(data2, 0, data2.Length);

                                states.Insert(pos, new KeyValuePair<long, StateEntity>(s.SequenceId, entity));
                            }
                        }

                // third, persist metadata and commit position
                key.Metadata = JsonConvert.SerializeObject(metadata);
                if (commitUpTo.HasValue && commitUpTo.Value > key.CommittedSequenceId) key.CommittedSequenceId = commitUpTo.Value;

                //if (string.IsNullOrEmpty(ETag)) await batchOperation.Add(TableOperation.Insert(this.key)).ConfigureAwait(false);
                //else await batchOperation.Add(TableOperation.Replace(this.key)).ConfigureAwait(false);

                var data = serializer.serialize(key);
                var sizeBytes = BitConverter.GetBytes(data.Length);
                await file.WriteAsync(sizeBytes, 0, sizeBytes.Length);
                await file.WriteAsync(data, 0, data.Length);

                // fourth, remove obsolete records
                if (states.Count > 0 && states[0].Key < obsoleteBefore)
                {
                    FindState(obsoleteBefore, out var pos);
                    //for (int i = 0; i < pos; i++) await batchOperation.Add(TableOperation.Delete(states[i].Value)).ConfigureAwait(false);
                    states.RemoveRange(0, pos);
                }

                //await batchOperation.Flush().ConfigureAwait(false);
                await file.FlushAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
            }
            finally
            {
                file.Close();
            }
            instanceLock.Release();
            return ETag;
        }

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
    }

    public class StateEntity
    {
        public string TransactionId;
        public DateTime TransactionTimestamp;
        public string TransactionManager;
        public string state;
    }

    public class KeyEntity
    {
        public long CommittedSequenceId = 0;
        public string Metadata;
    }
}