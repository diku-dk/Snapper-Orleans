using MessagePack;
using System.Collections.Generic;

namespace Utilities
{
    [MessagePackObject]
    public class WorkloadConfiguration
    {
        // benchmark setting
        [Key(0)]
        public int numEpochs;
        [Key(1)]
        public int numWarmupEpoch;
        [Key(2)]
        public int epochDurationMSecs;

        // workload config
        [Key(3)]
        public BenchmarkType benchmark;
        [Key(4)]
        public int txnSize;
        [Key(5)]
        public int actPipeSize;
        [Key(6)]
        public int pactPipeSize;
        [Key(7)]
        public Distribution distribution;
        [Key(8)]
        public float txnSkewness;
        [Key(9)]
        public float grainSkewness;
        [Key(10)]
        public float zipfianConstant;
        [Key(11)]
        public int pactPercent;
    }

    [MessagePackObject]
    public class WorkloadResult
    {
        [Key(0)]
        public int numDetTxn;
        [Key(1)]
        public int numNonDetTxn;
        [Key(2)]
        public int numDetCommitted;
        [Key(3)]
        public int numNonDetCommitted;
        [Key(4)]
        public long startTime;
        [Key(5)]
        public long endTime;
        [Key(6)]
        public int numNotSerializable;
        [Key(7)]
        public int numNotSureSerializable;
        [Key(8)]
        public int numDeadlock;

        [Key(9)]
        public List<double> latencies = new List<double>();
        [Key(10)]
        public List<double> det_latencies = new List<double>();

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