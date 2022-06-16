using MessagePack;

namespace Concurrency.Implementation.Logging
{
    public enum LogType { PREPARE, COMMIT, ABORT, DET_PREPARE, DET_COMPLETE, DET_COMMIT };

    [MessagePackObject]
    public class LogFormat
    {
        [Key(0)]
        public long sequenceNumber;
        [Key(1)]
        public readonly LogType logRecordType;
        [Key(2)]
        public readonly int coordinatorKey;
        [Key(3)]
        public readonly long tid;
        [Key(4)]
        public readonly byte[] state;     // this one may not be able to serialized by MsgPack!!!!!!!!!!!!!!

        public LogFormat(long sequenceNumber, LogType logRecordType, int coordinatorKey, long tid, byte[] state)
        {
            this.sequenceNumber = sequenceNumber;
            this.logRecordType = logRecordType;
            this.coordinatorKey = coordinatorKey;
            this.tid = tid;
            this.state = state;
        }

        public LogFormat(long sequenceNumber, LogType logRecordType, int coordinatorKey, long tid)
        {
            this.sequenceNumber = sequenceNumber;
            this.logRecordType = logRecordType;
            this.coordinatorKey = coordinatorKey;
            this.tid = tid;
        }
    }
}