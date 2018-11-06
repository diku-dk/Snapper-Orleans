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

        public Boolean isDeterministic;

        public Guid coordinatorKey;

        /*
         * Transaction coordinator sets the batchID and transactionID, which are not allowed to be changed.
         */
        public TransactionContext(int bid, int tid, Guid coordinatorKey)
        {
            batchID = bid;
            transactionID = tid;
            status = Status.Submitted;
            isDeterministic = true;
        }

        public TransactionContext(int tid, Guid coordinatorKey)
        {
            transactionID = tid;
            status = Status.Submitted;
            isDeterministic = false;
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
