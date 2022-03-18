using System;
using System.Diagnostics;

namespace Utilities
{
    [Serializable]
    public class TransactionRegistInfo
    {
        public readonly int tid;
        public readonly int bid;
        public readonly int highestCommittedBid;

        /// <summary> This constructor is only for PACT </summary>
        public TransactionRegistInfo(int bid, int tid, int highestCommittedBid)
        {
            this.tid = tid;
            this.bid = bid;
            this.highestCommittedBid = highestCommittedBid;
        }

        /// <summary> This constructor is only for ACT </summary>
        public TransactionRegistInfo(int tid, int highestCommittedBid)
        {
            this.tid = tid;
            bid = -1;
            this.highestCommittedBid = highestCommittedBid;
        }
    }

    [Serializable]
    public class TransactionContext
    {
        // only for PACT
        public int localBid;
        public int localTid;
        public readonly int globalBid;

        // for global PACT and all ACT
        public readonly int globalTid;

        // only for ACT: the grain who starts the ACT
        public readonly int nonDetCoordID;

        /// <summary> This constructor is only for local PACT </summary>
        public TransactionContext(int localTid, int localBid)
        {
            this.localTid = localTid;
            this.localBid = localBid;
            globalBid = -1;
            globalTid = -1;
            nonDetCoordID = -1;
        }

        /// <summary> This constructor is only for global PACT </summary>
        public TransactionContext(int localBid, int localTid, int globalBid, int globalTid)
        {
            this.localBid = localBid;
            this.localTid = localTid;
            this.globalBid = globalBid;
            this.globalTid = globalTid;
            nonDetCoordID = -1;
        }

        /// <summary> This constructor is only for ACT </summary>
        public TransactionContext(int globalTid, int nonDetCoordID, bool isDet)
        {
            Debug.Assert(isDet == false);
            localBid = -1;
            localTid = -1;
            globalBid = -1;
            this.globalTid = globalTid;
            this.nonDetCoordID = nonDetCoordID;
        }
    }
}