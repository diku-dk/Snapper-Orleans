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
            return ActivatorUtilities.CreateInstance<MemoryTransactionalStateStorage<TState>>(context.ActivationServices);
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
        private string ETag = "default";
        private long CommittedSequenceId = 0;
        private TransactionalStateMetaData Metadata;
        private List<KeyValuePair<long, StateEntity>> states;
        private ISerializer serializer;

        public MemoryTransactionalStateStorage()
        {
            states = new List<KeyValuePair<long, StateEntity>>();
            serializer = new MsgPackSerializer();
            Metadata = new TransactionalStateMetaData();
        }

        public async Task<TransactionalStorageLoadResponse<TState>> Load()
        {
            return new TransactionalStorageLoadResponse<TState>(ETag, new TState(), 0, Metadata, new List<PendingTransactionState<TState>>());
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
            /*
            if (this.key.ETag != expectedETag)
                throw new ArgumentException(nameof(expectedETag), "Etag does not match");

            // assemble all storage operations into a single batch
            // these operations must commit in sequence, but not necessarily atomically
            // so we can split this up if needed
            var batchOperation = new BatchOperation(logger, key, table);

            // first, clean up aborted records
            if (abortAfter.HasValue && states.Count != 0)
            {
                while (states.Count > 0 && states[states.Count - 1].Key > abortAfter)
                {
                    var entity = states[states.Count - 1].Value;
                    await batchOperation.Add(TableOperation.Delete(entity)).ConfigureAwait(false);
                    states.RemoveAt(states.Count - 1);

                    if (logger.IsEnabled(LogLevel.Trace))
                        logger.LogTrace($"{partition}.{entity.RowKey} Delete {entity.TransactionId}");
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
                            existing.TransactionManager = JsonConvert.SerializeObject(s.TransactionManager, this.jsonSettings);
                            existing.SetState(s.State, this.jsonSettings);
                            await batchOperation.Add(TableOperation.Replace(existing)).ConfigureAwait(false);

                            if (logger.IsEnabled(LogLevel.Trace))
                                logger.LogTrace($"{partition}.{existing.RowKey} Update {existing.TransactionId}");
                        }
                        else
                        {
                            var entity = StateEntity.Create(this.jsonSettings, this.partition, s);
                            await batchOperation.Add(TableOperation.Insert(entity)).ConfigureAwait(false);
                            states.Insert(pos, new KeyValuePair<long, StateEntity>(s.SequenceId, entity));

                            if (logger.IsEnabled(LogLevel.Trace))
                                logger.LogTrace($"{partition}.{entity.RowKey} Insert {entity.TransactionId}");
                        }
                    }

            // third, persist metadata and commit position
            key.Metadata = JsonConvert.SerializeObject(metadata, this.jsonSettings);
            if (commitUpTo.HasValue && commitUpTo.Value > key.CommittedSequenceId)
            {
                key.CommittedSequenceId = commitUpTo.Value;
            }
            if (string.IsNullOrEmpty(this.key.ETag))
            {
                await batchOperation.Add(TableOperation.Insert(this.key)).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Trace))
                    logger.LogTrace($"{partition}.{KeyEntity.RK} Insert. v{this.key.CommittedSequenceId}, {metadata.CommitRecords.Count}c");
            }
            else
            {
                await batchOperation.Add(TableOperation.Replace(this.key)).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Trace))
                    logger.LogTrace($"{partition}.{KeyEntity.RK} Update. v{this.key.CommittedSequenceId}, {metadata.CommitRecords.Count}c");
            }

            // fourth, remove obsolete records
            if (states.Count > 0 && states[0].Key < obsoleteBefore)
            {
                FindState(obsoleteBefore, out var pos);
                for (int i = 0; i < pos; i++)
                {
                    await batchOperation.Add(TableOperation.Delete(states[i].Value)).ConfigureAwait(false);

                    if (logger.IsEnabled(LogLevel.Trace))
                        logger.LogTrace($"{partition}.{states[i].Value.RowKey} Delete {states[i].Value.TransactionId}");
                }
                states.RemoveRange(0, pos);
            }

            await batchOperation.Flush().ConfigureAwait(false);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug($"{partition} Stored v{this.key.CommittedSequenceId} eTag={key.ETag}");
            
            return key.ETag;*/
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

    internal class StateEntity
    {
        public string TransactionId;
        public DateTime TransactionTimestamp;
        public byte[] TransactionManager;
        public byte[] state;
    }
}