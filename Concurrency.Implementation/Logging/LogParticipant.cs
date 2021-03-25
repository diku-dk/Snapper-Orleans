using System;
using System.Collections.Generic;

namespace Concurrency.Implementation.Logging
{
    [Serializable]
    public class LogParticipant
    {
        public int txn_id;
        public int sequenceNumber;
        public int coordinatorKey;
        public HashSet<Tuple<int, string>> participants;

        public LogParticipant(int sequenceNumber, int coordinatorKey, int txn_id, HashSet<Tuple<int, string>> grains)
        {
            this.sequenceNumber = sequenceNumber;
            this.coordinatorKey = coordinatorKey;
            this.txn_id = txn_id;
            participants = grains;
        }

    }
}
