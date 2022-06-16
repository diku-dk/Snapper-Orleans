using System.Collections.Generic;

namespace Utilities
{
    public class NonDeterministicBatchSchedule
    {
        public HashSet<int> txnList;

        public NonDeterministicBatchSchedule(int tid)
        {
            txnList = new HashSet<int>();
            txnList.Add(tid);
        }

        public void AddTransaction(int tid)
        {
            txnList.Add(tid);
        }

        public bool RemoveTransaction(int tid)
        {
            txnList.Remove(tid);
            return txnList.Count == 0;
        }
    }
}