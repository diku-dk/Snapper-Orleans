using System;
using System.Collections.Generic;
using System.Text;

namespace Concurrency.Implementation.Logging
{
    enum LogType { PREPARE, COMMIT, ABORT, DET_PREPARE, DET_COMPLETE, DET_COMMIT };
    
    [Serializable]
    class LogFormat<TState>
    {
        public int sequenceNumber;
        public LogType logRecordType;
        public Guid coordinatorKey;
        public int txn_id;
        public TState state;
        public LogFormat(int sequenceNumber, LogType logRecordType, Guid coordinatorKey, int txn_id, TState state)
        {
            this.sequenceNumber = sequenceNumber;
            this.logRecordType = logRecordType;
            this.coordinatorKey = coordinatorKey;
            this.txn_id = txn_id;
            this.state = state;
        }

        public LogFormat(int sequenceNumber, LogType logRecordType, Guid coordinatorKey, int txn_id)
        {
            this.sequenceNumber = sequenceNumber;
            this.logRecordType = logRecordType;
            this.coordinatorKey = coordinatorKey;
            this.txn_id = txn_id;
        }
    };
}
