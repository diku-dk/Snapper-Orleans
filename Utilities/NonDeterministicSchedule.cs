using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Utilities
{    
    public class NonDetBatchSchedule
    {
        public HashSet<int> transactions;
        public TaskCompletionSource<Boolean> waitingForBatch;

        NonDetBatchSchedule(TaskCompletionSource<Boolean> waitingForBatch, int transactionId)
        {
            transactions = new HashSet<int>();
            transactions.Add(transactionId);
            this.waitingForBatch = waitingForBatch;
        }

        public void AddTransaction(int transactionId)
        {
            transactions.Add(transactionId);            
        } 

        public Boolean RemoveTransaction(int transactionId)
        {
            transactions.Remove(transactionId);
            return (transactions.Count == 0);
        }
    }
}
