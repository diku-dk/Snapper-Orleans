using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Utilities
{    
    public class DeterministicBatchSchedule
    {
        private List<int> transactionList;
        private Dictionary<int, int> transactionAccessMap;
        private int curPos;
        private Boolean completed;
        public Guid globalCoordinator;
        public int batchID;
        public int lastBatchID;
        public int highestCommittedBatchId;

        public TaskCompletionSource<Boolean> prevDetBatch;
        public TaskCompletionSource<Boolean> prevNonDetBatch;


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

        public DeterministicBatchSchedule(DeterministicBatchSchedule schedule)
        {
            batchID = schedule.batchID;
            lastBatchID = schedule.lastBatchID;
            curPos = schedule.curPos;
            completed = schedule.completed;
            transactionList = new List<int>(schedule.transactionList);
            transactionAccessMap = new Dictionary<int, int>(schedule.transactionAccessMap);
        }        

        public void AddNewTransaction(int tid, int num)
        {
            transactionList.Add(tid);
            transactionAccessMap.Add(tid, num);

        }

        public int getLastTransaction()
        {
            if (transactionList == null)
                throw new ArgumentException("TRansactionLIst in Deterministic batch schedule must not be null.");
            else if (transactionList.Count == 0)
                throw new ArgumentException("TRansactionLIst in Deterministic batch schedule must not be empty.");
            else
                return transactionList[transactionList.Count - 1];
        }

        /**
         * Apply the transaction execution
         */

        public void AccessIncrement(int tid)
        {

            int num = --transactionAccessMap[tid];
            if (num == 0)
            {
                this.curPos++;
            }
            if (curPos == transactionList.Count)
            {
                completed = true;
            }
        }


        /**
         * Check if a transaction (tid) could be executed currently ot not.
         */
        public Boolean TryAccess(int tid)
        {
            if (completed)
                return false;
            if (this.transactionList[curPos] == tid)
                return true;
            else
                return false;
        }

        /**
         * Return the ID of the transaction that should be exected
         */

        public int curExecTransaction()
        {
            if (completed)
                return -1;
            else
                return transactionList[this.curPos];
        }

        public void setCompleted(Boolean b)
        {
            completed = b;
        }

    }
}
