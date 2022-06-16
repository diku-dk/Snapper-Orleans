using System;
using System.Diagnostics;

namespace Utilities
{
    [Serializable]
    public class TransactionRegistInfo
    {
        public readonly long tid;
        public readonly long bid;
        public readonly long highestCommittedBid;

        /// <summary> This constructor is only for PACT </summary>
        public TransactionRegistInfo(long bid, long tid, long highestCommittedBid)
        {
            this.tid = tid;
            this.bid = bid;
            this.highestCommittedBid = highestCommittedBid;
        }

        /// <summary> This constructor is only for ACT </summary>
        public TransactionRegistInfo(long tid, long highestCommittedBid)
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
        public long localBid;
        public long localTid;
        public readonly long globalBid;

        // for global PACT and all ACT
        public readonly long globalTid;

        // only for ACT: the grain who starts the ACT
        public readonly int nonDetCoordID;

        /// <summary> This constructor is only for local PACT </summary>
        public TransactionContext(long localTid, long localBid)
        {
            this.localTid = localTid;
            this.localBid = localBid;
            globalBid = -1;
            globalTid = -1;
            nonDetCoordID = -1;
        }

        /// <summary> This constructor is only for global PACT </summary>
        public TransactionContext(long localBid, long localTid, long globalBid, long globalTid)
        {
            this.localBid = localBid;
            this.localTid = localTid;
            this.globalBid = globalBid;
            this.globalTid = globalTid;
            nonDetCoordID = -1;
        }

        /// <summary> This constructor is only for ACT </summary>
        public TransactionContext(long globalTid, int nonDetCoordID, bool isDet)
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