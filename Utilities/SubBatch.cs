using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class SubBatch   // sent from global coordinator to local coordinator
    {
        public readonly int bid;
        public readonly int coordID;

        public int lastBid;
        public List<int> txnList;

        public SubBatch(int bid, int coordID)
        {
            lastBid = -1;
            this.bid = bid;
            this.coordID = coordID;
            txnList = new List<int>();
        }

        public SubBatch(SubBatch subBatch)
        {
            bid = subBatch.bid;
            coordID = subBatch.coordID;
            lastBid = subBatch.lastBid;
            txnList = subBatch.txnList;
        }
    }

    [Serializable]
    public class LocalSubBatch : SubBatch     // sent from local coordinator to transactional grains
    {
        public readonly int globalBid;
        public Dictionary<int, int> globalTidToLocalTid;

        public int highestCommittedBid;

        public LocalSubBatch(int globalBid, SubBatch subBatch) : base(subBatch)
        {
            this.globalBid = globalBid;
            globalTidToLocalTid = new Dictionary<int, int>();
            highestCommittedBid = -1;
        }
    }
}