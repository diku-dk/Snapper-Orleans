using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    public enum TxnType { DET, NONDET, HYBRID };
    public class WorkloadConfiguration
    {
        public int totalTransactions;
        public int numWorkerNodes;
        public int numThreadsPerWorkerNodes;
        public TxnType type;
    }

    public class WorkloadResults
    {
        public int[] numTxns;
        public int[] numSuccessFulTxns;
        public double[] startTime;
        public double[] endTime;
    }
}
