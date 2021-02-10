using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class DeterministicBatchSchedule
    {
        public List<int> transactionList;
        private Dictionary<int, int> transactionAccessMap;
        private int curPos;
        private bool completed;
        public int globalCoordinator;
        public int batchID;
        public int lastBatchID;
        public int highestCommittedBatchId;

        public DeterministicBatchSchedule(int bid, int lastBid)
        {
            batchID = bid;
            lastBatchID = lastBid;
            curPos = 0;
            completed = false;
            transactionList = new List<int>();
            transactionAccessMap = new Dictionary<int, int>();
        }

        public DeterministicBatchSchedule(int bid)
        {
            batchID = bid;
            lastBatchID = -1;
            curPos = 0;
            completed = false;
            transactionList = new List<int>();
            transactionAccessMap = new Dictionary<int, int>();
        }

        public void AddNewTransaction(int tid, int num)  // txn tid will access the grain num times
        {
            transactionList.Add(tid);
            transactionAccessMap.Add(tid, num);
        }

        public void AccessIncrement(int tid)  // txn tid finish one access
        {
            int num = --transactionAccessMap[tid];
            if (num == 0) curPos++;           // all accesses of this txn have finished, now skip to next txn
            if (curPos == transactionList.Count) completed = true;   // curPos = 0, 1, ... count - 1
        }

        public int curExecTransaction()  // return tid that should currently be exected
        {
            if (completed) return -1;
            else return transactionList[curPos];
        }
    }
}