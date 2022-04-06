using MessagePack;

namespace Concurrency.Implementation.Logging
{
    public enum LogType { PREPARE, COMMIT, ABORT, DET_PREPARE, DET_COMPLETE, DET_COMMIT };

    [MessagePackObject]
    public class LogFormat
    {
        [Key(0)]
        public int sequenceNumber;
        [Key(1)]
        public readonly LogType logRecordType;
        [Key(2)]
        public readonly int coordinatorKey;
        [Key(3)]
        public readonly int tid;
        [Key(4)]
        public readonly byte[] state;     // this one may not be able to serialized by MsgPack!!!!!!!!!!!!!!

        public LogFormat(int sequenceNumber, LogType logRecordType, int coordinatorKey, int tid, byte[] state)
        {
            this.sequenceNumber = sequenceNumber;
            this.logRecordType = logRecordType;
            this.coordinatorKey = coordinatorKey;
            this.tid = tid;
            this.state = state;
        }

        public LogFormat(int sequenceNumber, LogType logRecordType, int coordinatorKey, int tid)
        {
            this.sequenceNumber = sequenceNumber;
            this.logRecordType = logRecordType;
            this.coordinatorKey = coordinatorKey;
            this.tid = tid;
        }
    }
}