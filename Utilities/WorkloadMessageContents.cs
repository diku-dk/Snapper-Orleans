using System;
using System.Collections.Generic;

namespace Utilities
{
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
        public float zipfianConstant;
        public float deterministicTxnPercent;
        public ImplementationType grainImplementationType;
        public int[] percentilesToCalculate;

        // SmallBank specific configurations
        public int numAccounts;
        public int numAccountsPerGroup;
        public int[] mixture;                  // {getBalance, depositChecking, transder, transacSaving, writeCheck, multiTransfer}
        public int numAccountsMultiTransfer;
        public int numGrainsMultiTransfer;

        // TPCC specific configurations
        public int numWarehouse;
        public int numItemGrain;
        public int numOrderGrain;
        public int numStockGrain;
        public int numCustomerGrain;
        public int numDistrictGrain;
        public int numWarehouseGrain;
    }

    [Serializable]
    public class WorkloadResults
    {
        public int numDeadlock;
        public int numNotSerializable;
        public int numNotSureSerializable;
        public int numDetCommitted;
        public int numNonDetCommitted;
        public int numDetTxn;
        public int numNonDetTxn;
        public long startTime;
        public long endTime;
        public List<double> latencies;
        public List<double> det_latencies;

        public WorkloadResults(int numDetTxn, int numNonDetTxn, int numDetCommitted, int numNonDetCommitted, long startTime, long endTime, int numNotSerializable, int numNotSureSerializable, int numDeadlock)
        {
            this.numDetTxn = numDetTxn;
            this.numNonDetTxn = numNonDetTxn;
            this.numDetCommitted = numDetCommitted;
            this.numNonDetCommitted = numNonDetCommitted;
            this.startTime = startTime;
            this.endTime = endTime;
            this.numNotSerializable = numNotSerializable;
            this.numNotSureSerializable = numNotSureSerializable;
            this.numDeadlock = numDeadlock;
        }

        public void setLatency(List<double> latencies, List<double> det_latencies)
        {
            this.latencies = latencies;
            this.det_latencies = det_latencies;
        }
    }
}