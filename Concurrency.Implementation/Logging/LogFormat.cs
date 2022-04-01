using MessagePack;

namespace Concurrency.Implementation.Logging
{
    public enum LogType { PREPARE, COMMIT, ABORT, DET_PREPARE, DET_COMPLETE, DET_COMMIT };

    [MessagePackObject]
    public class LogFormat<TState>
    {
        [Key(0)]
        public int sequenceNumber;
        [Key(1)]
        public LogType logRecordType;
        [Key(2)]
        public int coordinatorKey;
        [Key(3)]
        public int txn_id;
        [Key(4)]
        public TState state;     // this one may not be able to serialized by MsgPack!!!!!!!!!!!!!!

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
    }
}