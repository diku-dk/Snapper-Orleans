using System;

namespace Concurrency.Implementation.Logging
{
    public enum LogType { PREPARE, COMMIT, ABORT, DET_PREPARE, DET_COMPLETE, DET_COMMIT };
    
    [Serializable]
    public class LogFormat<TState>
    {
        public int sequenceNumber;
        public LogType logRecordType;
        public int coordinatorKey;
        public int txn_id;
        public TState state;
        public LogFormat(int sequenceNumber, LogType logRecordType, int coordinatorKey, int txn_id, TState state)
        {
            this.sequenceNumber = sequenceNumber;
            this.logRecordType = logRecordType;
            this.coordinatorKey = coordinatorKey;
            this.txn_id = txn_id;
            this.state = state;
        }

        public LogFormat(int sequenceNumber, LogType logRecordType, int coordinatorKey, int txn_id)
        {
            this.sequenceNumber = sequenceNumber;
            this.logRecordType = logRecordType;
            this.coordinatorKey = coordinatorKey;
            this.txn_id = txn_id;
        }
    };
}
