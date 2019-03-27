using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Concurrency.Implementation.Logging
{
    [Serializable]
    class LogParticipant
    {
        public int sequenceNumber;
        public Guid coordinatorKey;
        public int txn_id;
        public HashSet<Guid> participants;

        public LogParticipant(int sequenceNumber, Guid coordinatorKey, int txn_id, HashSet<Guid> grains)
        {
            this.sequenceNumber = sequenceNumber;
            this.coordinatorKey = coordinatorKey;
            this.txn_id = txn_id;
            participants = grains;
        }

    }
}
