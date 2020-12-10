using System.Collections.Generic;

namespace Utilities
{
    public class NonDeterministicBatchSchedule
    {
        public HashSet<int> transactions;

        public NonDeterministicBatchSchedule(int tid)
        {
            transactions = new HashSet<int>();
            transactions.Add(tid);
        }

        public void AddTransaction(int tid)
        {
            transactions.Add(tid);
        }

        public bool RemoveTransaction(int tid)
        {
            transactions.Remove(tid);
            return (transactions.Count == 0);
        }
    }
}