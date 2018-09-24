using System;
using System.Collections.Generic;
using System.Text;

namespace Concurrency.Utilities
{
    [Serializable]
    public class TransactionContext
    {
        public int batchID { get; }
        public int transactionID { get; }

        public Status status;

        /*
         * Transaction coordinator sets the batchID and transactionID, which are not allowed to be changed.
         */
        public TransactionContext(int bid, int tid)
        {
            batchID = bid;
            transactionID = tid;
            status = Status.Submitted;
        }

        public TransactionContext(int tid)
        {
            transactionID = tid;
            status = Status.Submitted;
        }

        /*
         * State of a transaction
         */
        public enum Status
        {
            Submitted,
            Executing,
            Prepared,
            Aborted,
            Committed,
            Completed
        }

    }
}
