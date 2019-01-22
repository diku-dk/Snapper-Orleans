using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    [Serializable]
    public class BatchToken
    {
        public int lastBatchID { get; }
        public int lastTransactionID { get; }
     

        public BatchToken(int bid, int tid)
        {
            this.lastBatchID = bid;
            this.lastTransactionID = tid;
        }

    }
}
