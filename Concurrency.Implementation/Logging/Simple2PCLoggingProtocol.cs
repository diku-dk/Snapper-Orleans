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
        async Task ILoggingProtocol<TState>.HandleOnAbortIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey)
        {            
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.ABORT, coordinatorKey, tid);            
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.COMMIT, coordinatorKey, tid);
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
            
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.PREPARE, coordinatorKey, tid, state.GetPreparedState(tid));
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));            
        }

        async Task ILoggingProtocol<TState>.HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, long bid, Guid coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_COMPLETE, coordinatorKey, bid, state.GetCommittedState(bid));
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareInDeterministicProtocol(long bid)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_PREPARE, grainPrimaryKey, bid);
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitInDeterministicProtocol(long bid)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_COMMIT, grainPrimaryKey, bid);
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord)); 
        }
    }
}
