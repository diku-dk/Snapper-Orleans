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
        public static ISiloHostBuilder AddMemoryTransactionalStateStorage(this ISiloHostBuilder builder, string name, Action<OptionsBuilder<MemoryTransactionalStateOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddMemoryTransactionalStateStorage(name, configureOptions));
        }

        public static ISiloBuilder AddMemoryTransactionalStateStorage(this ISiloBuilder builder, string name, Action<OptionsBuilder<MemoryTransactionalStateOptions>> configureOptions = null)
        {
            return builder.ConfigureServices(services => services.AddMemoryTransactionalStateStorage(name, configureOptions));
        }

        private static IServiceCollection AddMemoryTransactionalStateStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<MemoryTransactionalStateOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<MemoryTransactionalStateOptions>());

            services.TryAddSingleton<ITransactionalStateStorageFactory>(sp => sp.GetServiceByName<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddSingletonNamedService<ITransactionalStateStorageFactory>(name, MemoryTransactionalStateStorageFactory.Create);
            services.AddSingletonNamedService<ILifecycleParticipant<ISiloLifecycle>>(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<ITransactionalStateStorageFactory>(n));

            return services;
        }
    }

    public class MemoryTransactionalStateOptions
    {
        public int InitStage { get; set; } = DEFAULT_INIT_STAGE;
        public const int DEFAULT_INIT_STAGE = ServiceLifecycleStage.ApplicationServices;
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
        private string ETag;
        private byte[] Metadata;
        private long CommittedSequenceId;
        private List<KeyValuePair<long, StateEntity>> states;
        private ISerializer serializer;

        public MemoryTransactionalStateStorage()
        {
            states = new List<KeyValuePair<long, StateEntity>>();
            serializer = new MsgPackSerializer();
        }

        public async Task<TransactionalStorageLoadResponse<TState>> Load()
        {
            Console.WriteLine("Loading from mem txnal storage!!!!!");
            if (string.IsNullOrEmpty(ETag)) return new TransactionalStorageLoadResponse<TState>();
            TState committedState;
            if (CommittedSequenceId == 0) committedState = new TState();
            else
            {
                if (!FindState(CommittedSequenceId, out var pos))
                {
                    var error = $"Storage state corrupted: no record for committed state v{CommittedSequenceId}";
                    throw new InvalidOperationException(error);
                }
                committedState = serializer.deserialize<TState>(states[pos].Value.state);
            }
            var PrepareRecordsToRecover = new List<PendingTransactionState<TState>>();
            for (int i = 0; i < states.Count; i++)
            {
                var kvp = states[i];

                // pending states for already committed transactions can be ignored
                if (kvp.Key <= CommittedSequenceId) continue;

                // upon recovery, local non-committed transactions are considered aborted
                if (kvp.Value.TransactionManager == null) break;

                ParticipantId tm = serializer.deserialize<ParticipantId>(kvp.Value.TransactionManager);

                PrepareRecordsToRecover.Add(new PendingTransactionState<TState>()
                {
                    SequenceId = kvp.Key,
                    State = serializer.deserialize<TState>(kvp.Value.state),
                    TimeStamp = kvp.Value.TransactionTimestamp,
                    TransactionId = kvp.Value.TransactionId,
                    TransactionManager = tm
                });

            }
            // clear the state strings... no longer needed, ok to GC now
            for (int i = 0; i < states.Count; i++) states[i].Value.state = null;
            TransactionalStateMetaData metadata = serializer.deserialize<TransactionalStateMetaData>(Metadata);
            return new TransactionalStorageLoadResponse<TState>(ETag, committedState, CommittedSequenceId, metadata, PrepareRecordsToRecover);
        }

        public async Task<string> Store(string expectedETag, TransactionalStateMetaData metadata, List<PendingTransactionState<TState>> statesToPrepare, long? commitUpTo, long? abortAfter)
        {
            if (ETag != expectedETag) throw new ArgumentException(nameof(expectedETag), "Etag does not match");

            // first, clean up aborted records
            if (abortAfter.HasValue && states.Count != 0)
            {
                while (states.Count > 0 && states[states.Count - 1].Key > abortAfter)
                    states.RemoveAt(states.Count - 1);
            }

            // second, persist non-obsolete prepare records
            var obsoleteBefore = commitUpTo.HasValue ? commitUpTo.Value : CommittedSequenceId;
            if (statesToPrepare != null)
            {
                foreach (var s in statesToPrepare)
                {
                    if (s.SequenceId >= obsoleteBefore)
                    {
                        if (FindState(s.SequenceId, out var pos))
                        {
                            // overwrite with new pending state
                            StateEntity existing = states[pos].Value;
                            existing.TransactionId = s.TransactionId;
                            existing.TransactionTimestamp = s.TimeStamp;
                            existing.TransactionManager = serializer.serialize(s.TransactionManager);
                            existing.state = serializer.serialize(s.State);
                        }
                        else
                        {
                            var entity = new StateEntity();
                            states.Insert(pos, new KeyValuePair<long, StateEntity>(s.SequenceId, entity));
                        }
                    }
                }
            }

            // third, persist metadata and commit position
            Metadata = serializer.serialize(metadata);
            if (commitUpTo.HasValue && commitUpTo.Value > CommittedSequenceId) CommittedSequenceId = commitUpTo.Value;

            // fourth, remove obsolete records
            if (states.Count > 0 && states[0].Key < obsoleteBefore)
            {
                FindState(obsoleteBefore, out var pos);
                states.RemoveRange(0, pos);
            }
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