using System;
using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;

namespace Concurrency.Implementation.Logging
{
    class Simple2PCLoggingProtocol<TState> : ILoggingProtocol<TState>
    {
        private int grainID;
        private int sequenceNumber;
        private ISerializer serializer;
        private IKeyValueStorageWrapper logStorage;

        public Simple2PCLoggingProtocol(string grainType, int grainID, dataFormatType dataFormat, StorageWrapperType storage)
        {
            this.grainID = grainID;
            sequenceNumber = 0;
            switch (storage)
            {
                case StorageWrapperType.FILESYSTEM:
                    logStorage = new FileKeyValueStorageWrapper(grainType, grainID);
                    break;
                case StorageWrapperType.DYNAMODB:
                    logStorage = new DynamoDBStorageWrapper(grainType, grainID);
                    break;
                case StorageWrapperType.INMEMORY:
                    logStorage = new InMemoryStorageWrapper();
                    break;
                default:
                    throw new Exception($"Exception: Unknown StorageWrapper {storage}");
            }

            switch (dataFormat)
            {
                case dataFormatType.BINARY:
                    serializer = new BinarySerializer();
                    break;
                case dataFormatType.MSGPACK:
                    serializer = new MsgPackSerializer();
                    break;
                default:
                    throw new Exception($"Exception: Unknown serailizer {dataFormat}");
            }
        }

        private int getSequenceNumber()
        {
            int returnVal;
            lock (this)
            {
                returnVal = sequenceNumber;
                sequenceNumber++;
            }
            return returnVal;
        }
        async Task ILoggingProtocol<TState>.HandleBeforePrepareIn2PC(int tid, int coordinatorKey, HashSet<int> grains)
        {
            var logRecord = new LogParticipant(getSequenceNumber(), coordinatorKey, tid, grains);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), serializer.serialize(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnAbortIn2PC(int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.ABORT, coordinatorKey, tid);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), serializer.serialize(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitIn2PC(int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.COMMIT, coordinatorKey, tid);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), serializer.serialize(logRecord));

        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareIn2PC(ITransactionalState<TState> state, int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.PREPARE, coordinatorKey, tid, state.GetPreparedState(tid));
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), serializer.serialize(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, int bid, int coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_COMPLETE, coordinatorKey, bid, state.GetCommittedState(bid));
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), serializer.serialize(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareInDeterministicProtocol(int bid, HashSet<int> grains)
        {
            var logRecord = new LogParticipant(getSequenceNumber(), grainID, bid, grains);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), serializer.serialize(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitInDeterministicProtocol(int bid)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_COMMIT, grainID, bid);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), serializer.serialize(logRecord));
        }

        // if persist PACT input
        async Task ILoggingProtocol<TState>.HandleOnPrepareInDeterministicProtocol(int bid, Dictionary<int, DeterministicBatchSchedule> batchSchedule, Dictionary<int, Tuple<int, object>> inputs)
        {
            var logRecord = new LogForPACT(getSequenceNumber(), bid, batchSchedule, inputs);
            await logStorage.Write(BitConverter.GetBytes(logRecord.sequenceNumber), serializer.serialize(logRecord));
        }
    }
}
