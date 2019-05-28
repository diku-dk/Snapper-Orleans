using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
  
    public enum BenchmarkType {SMALLBANK};
    public enum ImplementationType { SNAPPER, ORLEANSEVENTUAL, ORLEANSTXN };
    public enum Distribution { ZIPFIAN, UNIFORM}

    [Serializable]
    public class WorkloadConfiguration
    {
        public int epochInMiliseconds;
        public int numEpoch;
        public int numWorkerNodes;
        public int numThreadsPerWorkerNodes;
        public BenchmarkType benchmark;
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
        public ImplementationType grainImplementationType;
    }

    [Serializable]
    public class WorkloadResults
    {
        public int numCommitted;
        public int numTransactions;
        public long startTime;
        public long endTime;
        public List<long> latencies;

        public WorkloadResults(int numTransactions, int numCommitted, long startTime, long endTime, List<long> latencies)
        {
            this.numTransactions = numTransactions;
            this.numCommitted = numCommitted;
            this.startTime = startTime;
            this.endTime = endTime;
            this.latencies = latencies;
        }


    }

    [Serializable]
    public class AggregatedWorkloadResults
    {
        public List<List<WorkloadResults>> results;

        public AggregatedWorkloadResults(List<WorkloadResults>[] input)
        {
            results = new List<List<WorkloadResults>>();
            for(int i=0; i<input.Length; i++)
            {
                results.Add(input[i]);
            }
        }


    }
}
