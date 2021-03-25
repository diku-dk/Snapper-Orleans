using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class BatchToken
    {
        public int lastCoordID;
        public int lastBatchID { get; set; }
        public int lastTransactionID { get; set; }
        public Dictionary<string, Dictionary<int, int>> lastBatchPerGrain { get; set; }   // <grain namespace, grainID, bid>
        public int highestCommittedBatchID = -1;

        // token back off mechanism
        public bool idleToken;
        public bool backoff;
        public int markedIdleByCoordinator;
        public int backOffProbeStartTime;

        public BatchToken(int bid, int tid)
        {
            lastBatchID = bid;
            lastTransactionID = tid;
            idleToken = false;
            backoff = true;
            lastBatchPerGrain = new Dictionary<string, Dictionary<int, int>>();
        }
    }
}
