using System;
using Utilities;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Interface.TransactionExecution;

namespace Concurrency.Implementation.Logging
{
    class Simple2PCLoggingProtocol<TState> : ILoggingProtocol<TState>
    {
        private int grainID;
        private int sequenceNumber;
        private ISerializer serializer;
        private IKeyValueStorageWrapper logStorage;

        private bool useLogger = false;
        private ILogger logger;

        public Simple2PCLoggingProtocol(string grainType, int grainID, object logger = null)
        {
            this.grainID = grainID;
            sequenceNumber = 0;

            switch (Constants.loggingType)
            {
                case LoggingType.NOLOGGING:
                    break;
                case LoggingType.ONGRAIN:
                    switch (Constants.storageType)
                    {
                        case StorageType.FILESYSTEM:
                            logStorage = new FileKeyValueStorageWrapper(grainType, grainID);
                            break;
                        case StorageType.DYNAMODB:
                            logStorage = new DynamoDBStorageWrapper(grainType, grainID);
                            break;
                        case StorageType.INMEMORY:
                            logStorage = new InMemoryStorageWrapper();
                            break;
                        default:
                            throw new Exception($"Exception: Unknown StorageWrapper {Constants.storageType}");
                    }
                    break;
                case LoggingType.LOGGER:
                    useLogger = true;
                    Debug.Assert(logger != null);
                    this.logger = (ILogger)logger;
                    break;
                default:
                    throw new Exception($"Exception: Unknown loggingType {Constants.loggingType}");
            }

            switch (Constants.serializerType)
            {
                case SerializerType.BINARY:
                    serializer = new BinarySerializer();
                    break;
                case SerializerType.MSGPACK:
                    serializer = new MsgPackSerializer();
                    break;
                default:
                    throw new Exception($"Exception: Unknown serailizer {Constants.serializerType}");
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

        private async Task WriteLog(byte[] key, byte[] value)
        {
            if (useLogger) await logger.Write(value);
            else await logStorage.Write(key, value);
        }

        public async Task HandleBeforePrepareIn2PC(int tid, int coordinatorKey, HashSet<int> grains)
        {
            var logRecord = new LogParticipant(getSequenceNumber(), coordinatorKey, tid, grains);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = serializer.serialize(logRecord);
            await WriteLog(key, value);
        }

        public async Task HandleOnAbortIn2PC(int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.ABORT, coordinatorKey, tid);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = serializer.serialize(logRecord);
            await WriteLog(key, value);
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitIn2PC(int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.COMMIT, coordinatorKey, tid);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = serializer.serialize(logRecord);
            await WriteLog(key, value);
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareIn2PC(ITransactionalState<TState> state, int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.PREPARE, coordinatorKey, tid, state.GetPreparedState(tid));
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = serializer.serialize(logRecord);
            await WriteLog(key, value);
        }

        async Task ILoggingProtocol<TState>.HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, int bid, int coordinatorKey)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_COMPLETE, coordinatorKey, bid, state.GetCommittedState(bid));
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = serializer.serialize(logRecord);
            await WriteLog(key, value);
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareInDeterministicProtocol(int bid, HashSet<int> grains)
        {
            var logRecord = new LogParticipant(getSequenceNumber(), grainID, bid, grains);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = serializer.serialize(logRecord);
            await WriteLog(key, value);
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitInDeterministicProtocol(int bid)
        {
            var logRecord = new LogFormat<TState>(getSequenceNumber(), LogType.DET_COMMIT, grainID, bid);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = serializer.serialize(logRecord);
            await WriteLog(key, value);
        }
    }
}