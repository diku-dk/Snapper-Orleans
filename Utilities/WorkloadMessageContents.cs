using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class WorkloadConfiguration
    {
        // benchmarkframework setting
        public int numEpochs;
        public int epochDurationMSecs;
        public int numThreadsPerWorkerNode;
        public int numConnToClusterPerWorkerNode;
        public int[] percentilesToCalculate;

        // workload config
        public BenchmarkType benchmark;
        public int txnSize;
        public int actPipeSize;
        public int pactPipeSize;
        public Distribution distribution;
        public float txnSkewness;
        public float grainSkewness;
        public float zipfianConstant;
        public int pactPercent;
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

        public List<double> startTxnTime;
        public List<double> update1Time;
        public List<double> update2Time;
        public List<double> endTxnTime;

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

        public void setBreakdownLatency(List<double> startTxnTime, List<double> update2Time, List<double> update1Time, List<double> endTxnTime)
        {
            this.startTxnTime = startTxnTime;
            this.update1Time = update1Time;
            this.update2Time = update2Time;
            this.endTxnTime = endTxnTime;
        }
    }
}