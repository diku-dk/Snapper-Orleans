using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class DeterministicBatchSchedule
    {
        public int bid;
        private int curPos;
        public int coordID;
        public int lastBid;
        private bool completed;
        public List<int> txnList;
        public int highestCommittedBid;
        private Dictionary<int, int> txnAccessMap;

        public DeterministicBatchSchedule(int bid, int lastBid)
        {
            this.bid = bid;
            this.lastBid = lastBid;
            curPos = 0;
            completed = false;
            txnList = new List<int>();
            txnAccessMap = new Dictionary<int, int>();
        }

        public DeterministicBatchSchedule(int bid)
        {
            this.bid = bid;
            lastBid = -1;
            curPos = 0;
            completed = false;
            txnList = new List<int>();
            txnAccessMap = new Dictionary<int, int>();
        }

        public void AddNewTransaction(int tid, int num)  // txn tid will access the grain num times
        {
            txnList.Add(tid);
            txnAccessMap.Add(tid, num);
        }

        public void AccessIncrement(int tid)  // txn tid finish one access
        {
            int num = --txnAccessMap[tid];
            if (num == 0) curPos++;           // all accesses of this txn have finished, now skip to next txn
            if (curPos == txnList.Count) completed = true;   // curPos = 0, 1, ... count - 1
        }

        public int curExecTransaction()  // return tid that should currently be exected
        {
            if (completed) return -1;
            else return txnList[curPos];
        }
    }
}