using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class WorkloadConfiguration
    {
        // benchmark setting
        public int numEpochs;
        public int numWarmupEpoch;
        public int epochDurationMSecs;

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
    public class WorkloadResult
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

        public WorkloadResult(int numDetTxn, int numNonDetTxn, int numDetCommitted, int numNonDetCommitted, long startTime, long endTime, int numNotSerializable, int numNotSureSerializable, int numDeadlock)
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