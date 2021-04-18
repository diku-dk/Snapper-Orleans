using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class TransactionContext
    {
        public int bid;
        public int tid;
        public bool isDet;
        public int coordID;
        public int highestCommittedBid;
        public Dictionary<int, Tuple<string, int>> grainAccessInfo;  // <grainID, namespace, access this grian how many times>

        public TransactionContext(int tid)
        {
            this.tid = tid;
            isDet = false;
            highestCommittedBid = -1;
        }

        public TransactionContext(Dictionary<int, Tuple<string, int>> grainAccessInfo)
        {
            isDet = true;
            highestCommittedBid = -1;
            this.grainAccessInfo = grainAccessInfo;
        }
    }
}