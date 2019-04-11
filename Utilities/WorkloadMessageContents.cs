using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    public enum TxnType { DET, NONDET, HYBRID };

    [Serializable]
    public class WorkloadConfiguration 
    {
        public int totalTransactions;
        public int numWorkerNodes;
        public int numThreadsPerWorkerNodes;
        public TxnType type;
    }

    [Serializable]
    public class WorkloadResults
    {
        public int numTxns;
        public int numSuccessFulTxns;
        public long minLatency;
        public long maxLatency;
        public long averageLatency;
        public int throughput;

        public WorkloadResults()
        {
        }

        public WorkloadResults(int numTxns, int numSuccessFulTxns, long minLatency, long maxLatency, long averageLatency, int throughput)
        {
            this.numTxns = numTxns;
            this.numSuccessFulTxns = numSuccessFulTxns;
            this.minLatency = minLatency;
            this.maxLatency = maxLatency;
            this.averageLatency = averageLatency;
            this.throughput = throughput;
        }
    }
}
