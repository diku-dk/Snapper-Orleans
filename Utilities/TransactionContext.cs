﻿using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class TransactionContext
    {
        // if persist PACT input
        public int grainID;  // the grain who starts the transaction
        public object input;

        public int coordinatorKey;
        public bool isDeterministic;
        public int batchID { get; set; }
        public int highestBatchIdCommitted;
        public int transactionID { get; set; }
        public Dictionary<Tuple<int, string>, int> grainAccessInformation;  // <grainID, access this grian how many times>

        public TransactionContext(int tid)
        {
            transactionID = tid;
            isDeterministic = false;
            highestBatchIdCommitted = -1;
        }

        public TransactionContext(Dictionary<Tuple<int, string>, int> grainAccessInformation)
        {
            this.grainAccessInformation = grainAccessInformation;
            isDeterministic = true;
            highestBatchIdCommitted = -1;
        }
    }
}
