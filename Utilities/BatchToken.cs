using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class BatchToken
    {
        public int lastBid;
        public int lastTid;
        public int lastCoordID;
        public int highestCommittedBid = -1;
        public Dictionary<int, Tuple<string, int>> lastBidPerGrain;   // <actorID, namespace, bid>

        public BatchToken(int bid, int tid)
        {
            lastBid = bid;
            lastTid = tid;
            lastBidPerGrain = new Dictionary<int, Tuple<string, int>>();
        }
    }
}