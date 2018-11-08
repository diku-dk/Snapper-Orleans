using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;

namespace Concurrency.Implementation.Logging
{
    [Serializable]
    class LogParticipant
    {
        public long sequenceNumber;
        public Guid coordinatorKey;
        public long txn_id;
        public HashSet<Guid> participants;

        public LogParticipant(long sequenceNumber, Guid coordinatorKey, long txn_id, HashSet<Guid> grains)
        {
            this.sequenceNumber = sequenceNumber;
            this.coordinatorKey = coordinatorKey;
            this.txn_id = txn_id;
            participants = grains;
        }

    }
}
