using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    [Serializable]
    public class BatchToken
    {
        public int lastBatchID { get; set; }
        public int lastTransactionID { get; set; }
     

        public BatchToken(int bid, int tid)
        {
            this.lastBatchID = bid;
            this.lastTransactionID = tid;
        }



    }
}
