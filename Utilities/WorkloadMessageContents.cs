using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    public enum TxnType { DET, NONDET, HYBRID };
    public enum Benchmark {SMALLBANK};
    public enum Distribution { ZIPFIAN, UNIFORM}

    [Serializable]
    public class WorkloadConfiguration
    {
        public int totalTransactions;
        public int numWorkerNodes;
        public int numThreadsPerWorkerNodes;
        public TxnType type;
        public Benchmark benchmark;
        public Distribution distribution;

        //SmallBank Specific configurations
        public uint numGroups;
        public uint numAccounts;
        public uint numAccountsPerGroup;
        public int[] mixture;//{getBalance, depositChecking, transder, transacSaving, writeCheck, multiTransfer}
        public int numAccountsMultiTransfer;
        public int numGrainsMultiTransfer;
        public float zipf;
        public float deterministicTxnPercent;
    }

    [Serializable]
    public class WorkloadResults
    {
        public int numTxns;
        public int numSuccessFulTxns;
        public long minLatency;
        public long maxLatency;
        public long averageLatency;
        public float throughput;

        public WorkloadResults(int numTxns, int numSuccessFulTxns, long minLatency, long maxLatency, long averageLatency, float throughput)
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
