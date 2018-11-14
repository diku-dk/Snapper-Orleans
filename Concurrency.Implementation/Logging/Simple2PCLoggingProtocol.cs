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
        String grainType;
        Guid grainPrimaryKey;
        long sequenceNumber;

        public Simple2PCLoggingProtocol(String grainType, Guid grainPrimaryKey) {
            this.grainType = grainType;
            this.grainPrimaryKey = grainPrimaryKey;
            this.sequenceNumber = 0;
            const string basePath = @"C:\Users\x\orleans-logs\";
            logStorage = new FileKeyValueStorageWrapper(basePath, grainType, grainPrimaryKey.ToString());
            //logStorage = new DynamoDBStorageWrapper(grainType, grainPrimaryKey.ToString());
        }

        private long getSequenceNumber()
        {
            return sequenceNumber++;
        }
        async Task ILoggingProtocol<TState>.HandleBeforePrepareIn2PC(long tid, Guid coordinatorKey, HashSet<Guid> grains)
        {
            var logRecord = new LogParticipant(getSequenceNumber(), grainPrimaryKey, tid, grains);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), Helper.serializeToByteArray<LogParticipant>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnAbortIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey)
        {            
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.ABORT, coordinatorKey, tid);            
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.COMMIT, coordinatorKey, tid);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
            
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.PREPARE, coordinatorKey, tid, state.GetPreparedState(tid));
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), Helper.serializeToByteArray<LogFormat<TState>>(logRecord));            
        }

        async Task ILoggingProtocol<TState>.HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, long bid, Guid coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_COMPLETE, coordinatorKey, bid, state.GetCommittedState(bid));
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareInDeterministicProtocol(long bid, HashSet<Guid> grains)
        {
            var logRecord = new LogParticipant(getSequenceNumber(), grainPrimaryKey, bid, grains);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), Helper.serializeToByteArray<LogParticipant>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitInDeterministicProtocol(long bid)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_COMMIT, grainPrimaryKey, bid);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), Helper.serializeToByteArray<LogFormat<TState>>(logRecord)); 
        }


    }
}
