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
        public int numConnToClusterPerWorkerNode;
        public int numWorkerNodes;
        public int numThreadsPerWorkerNode;
        public int asyncMsgLengthPerThread;
        public BenchmarkType benchmark;
        public int numEpochs;
        public int epochDurationMSecs;
        public Distribution distribution;
        //SmallBank Specific configurations
        public uint numAccounts;
        public uint numAccountsPerGroup;
        public int[] mixture;//{getBalance, depositChecking, transder, transacSaving, writeCheck, multiTransfer}
        public int numAccountsMultiTransfer;
        public int numGrainsMultiTransfer;
        public float zipfianConstant;
        public float deterministicTxnPercent;
        public ImplementationType grainImplementationType;
        public int[] percentilesToCalculate;
    }

    [Serializable]
    public class WorkloadResults
    {
        public int[] abortType;
        public int numCommitted;
        public int numTransactions;
        public int numNonDetTxn;
        public long startTime;
        public long endTime;
        public List<double> latencies;

        public WorkloadResults(int numTransactions, int numCommitted, int numNonDetTxn, long startTime, long endTime, List<double> latencies, int[] abortType)
        {
            this.numTransactions = numTransactions;
            this.numCommitted = numCommitted;
            this.numNonDetTxn = numNonDetTxn;
            this.startTime = startTime;
            this.endTime = endTime;
            this.latencies = latencies;
            this.abortType = abortType;
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
