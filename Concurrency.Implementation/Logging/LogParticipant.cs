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
        public Dictionary<string, HashSet<int>> participants;

        public LogParticipant(int sequenceNumber, int coordinatorKey, int txn_id, Dictionary<string, HashSet<int>> grains)
        {
            this.sequenceNumber = sequenceNumber;
            this.coordinatorKey = coordinatorKey;
            this.txn_id = txn_id;
            participants = grains;
        }

    }
}
