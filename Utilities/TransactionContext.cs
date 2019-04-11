using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    [Serializable]
    public class TransactionContext
    {
        public int batchID { get; set; }
        public int transactionID { get; set; }

        public Status status;

        public Boolean isDeterministic;

        public Guid coordinatorKey;

        public Dictionary<Guid, Tuple<string, int>> grainAccessInformation;

        public int highestBatchIdCommitted;

        /*
         * Transaction coordinator sets the batchID and transactionID, which are not allowed to be changed.
         */
        public TransactionContext(int bid, int tid, Guid coordinatorKey)
        {
            batchID = bid;
            transactionID = tid;
            status = Status.Submitted;
            isDeterministic = true;
            highestBatchIdCommitted = -1;
        }

        public TransactionContext(int tid)
        {
            transactionID = tid;
            status = Status.Submitted;
            isDeterministic = false;
            highestBatchIdCommitted = -1;
        }

        public TransactionContext(Dictionary<Guid, Tuple<string, int>> grainAccessInformation)
        {
            this.grainAccessInformation = grainAccessInformation;
            status = Status.Submitted;
            isDeterministic = true;
            highestBatchIdCommitted = -1;
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
