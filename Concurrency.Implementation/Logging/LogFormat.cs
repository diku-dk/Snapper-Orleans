using System;
using System.Collections.Generic;
using Utilities;

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
    }

    // if persist PACT input
    [Serializable]
    public class LogForPACT
    {
        public int sequenceNumber;

        public int bid;
        public Dictionary<int, DeterministicBatchSchedule> batchSchedule;
        public Dictionary<int, Tuple<int, object>> inputs;

        public LogForPACT(int sequenceNumber, int bid, Dictionary<int, DeterministicBatchSchedule> batchSchedule, Dictionary<int, Tuple<int, object>> inputs)
        {
            this.sequenceNumber = sequenceNumber;
            this.bid = bid;
            this.batchSchedule = batchSchedule;
            this.inputs = inputs;
        }
    }
}
