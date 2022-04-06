using System;
using Utilities;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using MessagePack;

namespace Concurrency.Implementation.Logging
{
    public class LoggingProtocol : ILoggingProtocol
    {
        int sequenceNumber;
        readonly int grainID;
        readonly ILogger logger;
        readonly bool useLogger = false;
        readonly IKeyValueStorageWrapper logStorage;

        public LoggingProtocol(string grainType, int grainID, object logger = null)
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
        }

        int getSequenceNumber()
        {
            int returnVal;
            lock (this)
            {
                returnVal = sequenceNumber;
                sequenceNumber++;
            }
            return returnVal;
        }

        async Task WriteLog(byte[] key, byte[] value)
        {
            if (useLogger) await logger.Write(value);
            else await logStorage.Write(key, value);
        }

        public async Task HandleBeforePrepareIn2PC(int tid, int coordinatorKey, HashSet<int> grains)
        {
            var logRecord = new LogParticipant(getSequenceNumber(), coordinatorKey, tid, grains);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = MessagePackSerializer.Serialize(logRecord);
            await WriteLog(key, value);
        }

        public async Task HandleOnAbortIn2PC(int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat(getSequenceNumber(), LogType.ABORT, coordinatorKey, tid);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = MessagePackSerializer.Serialize(logRecord);
            await WriteLog(key, value);
        }

        public async Task HandleOnCommitIn2PC(int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat(getSequenceNumber(), LogType.COMMIT, coordinatorKey, tid);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = MessagePackSerializer.Serialize(logRecord);
            await WriteLog(key, value);
        }

        public async Task HandleOnPrepareIn2PC(byte[] state, int tid, int coordinatorKey)
        {
            var logRecord = new LogFormat(getSequenceNumber(), LogType.PREPARE, coordinatorKey, tid, state);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = MessagePackSerializer.Serialize(logRecord);
            await WriteLog(key, value);
        }

        public async Task HandleOnCompleteInDeterministicProtocol(byte[] state, int bid, int coordinatorKey)
        {
            var logRecord = new LogFormat(getSequenceNumber(), LogType.DET_COMPLETE, coordinatorKey, bid, state);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = MessagePackSerializer.Serialize(logRecord);
            await WriteLog(key, value);
        }

        public async Task HandleOnPrepareInDeterministicProtocol(int bid, HashSet<int> grains)
        {
            var logRecord = new LogParticipant(getSequenceNumber(), grainID, bid, grains);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = MessagePackSerializer.Serialize(logRecord);
            await WriteLog(key, value);
        }

        public async Task HandleOnCommitInDeterministicProtocol(int bid)
        {
            var logRecord = new LogFormat(getSequenceNumber(), LogType.DET_COMMIT, grainID, bid);
            var key = BitConverter.GetBytes(logRecord.sequenceNumber);
            var value = MessagePackSerializer.Serialize(logRecord);
            await WriteLog(key, value);
        }
    }
}