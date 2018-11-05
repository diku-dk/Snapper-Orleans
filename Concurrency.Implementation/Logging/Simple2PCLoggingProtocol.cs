using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Nondeterministic;
using Utilities;

namespace Concurrency.Implementation.Logging
{
    class Simple2PCLoggingProtocol<TState> : ILoggingProtocol<TState>
    {
        IKeyValueStorageWrapper logStorage;
        Guid grainPrimaryKey;
        long sequenceNumber;

        public Simple2PCLoggingProtocol(Guid grainPrimaryKey) {
            this.grainPrimaryKey = grainPrimaryKey;
            this.sequenceNumber = 0;
            const string basePath = @"C:\Users\x\orleans-logs\";
            logStorage = new FileKeyValueStorageWrapper(basePath);
        }

        private long getSequenceNumber()
        {
            return sequenceNumber++;
        }
        async Task ILoggingProtocol<TState>.HandleOnAbortIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator)
        {            
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.ABORT, onCoordinator, tid);            
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.COMMIT, onCoordinator, tid);
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
            
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.PREPARE, onCoordinator, tid, state.GetPreparedState(tid));
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));            
        }
    }
}
