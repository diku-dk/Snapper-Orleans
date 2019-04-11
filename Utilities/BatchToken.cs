using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    [Serializable]
    public class BatchToken
    {
        public int lastBatchID { get; set; }
        public int lastTransactionID { get; set; }
        public Dictionary<Guid, int> lastBatchPerGrain { get; set; }
        public int highestCommittedBatchID = -1;
        public bool idleToken;
        public bool backoff;
        public Guid markedIdleByCoordinator;
        public int backOffProbeStartTime;


        public BatchToken(int bid, int tid)
        {
            this.lastBatchID = bid;
            this.lastTransactionID = tid;
            idleToken = false;
            backoff = true;
            lastBatchPerGrain = new Dictionary<Guid, int>();
        }
    }
}
