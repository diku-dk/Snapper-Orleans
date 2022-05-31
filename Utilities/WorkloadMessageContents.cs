using System.Collections.Generic;
using MessagePack;

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
        public List<double> dist_latencies = new List<double>();
        [Key(11)]
        public List<double> non_dist_latencies = new List<double>();

        [Key(12)]
        public List<double> dist_prepareTxnTime = new List<double>();     // grain receive txn  ==>  start execute txn
        [Key(13)]
        public List<double> dist_executeTxnTime = new List<double>();     // start execute txn  ==>  finish execute txn
        [Key(14)]
        public List<double> dist_commitTxnTime = new List<double>();      // finish execute txn ==>  batch committed

        [Key(15)]
        public List<double> non_dist_prepareTxnTime = new List<double>();     // grain receive txn  ==>  start execute txn
        [Key(16)]
        public List<double> non_dist_executeTxnTime = new List<double>();     // start execute txn  ==>  finish execute txn
        [Key(17)]
        public List<double> non_dist_commitTxnTime = new List<double>();      // finish execute txn ==>  batch committed

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

        public void setLatency(List<double> latencies, List<double> dist_latencies, List<double> non_dist_latencies)
        {
            this.latencies = latencies;
            this.dist_latencies = dist_latencies;
            this.non_dist_latencies = non_dist_latencies;
        }

        public void setBreakdownLatency(
            List<double> dist_prepareTxnTime,
            List<double> dist_executeTxnTime,
            List<double> dist_commitTxnTime,
            List<double> non_dist_prepareTxnTime,
            List<double> non_dist_executeTxnTime,
            List<double> non_dist_commitTxnTime)
        {
            this.dist_prepareTxnTime = dist_prepareTxnTime;
            this.dist_executeTxnTime = dist_executeTxnTime;
            this.dist_commitTxnTime = dist_commitTxnTime;
            this.non_dist_prepareTxnTime = non_dist_prepareTxnTime;
            this.non_dist_executeTxnTime = non_dist_executeTxnTime;
            this.non_dist_commitTxnTime = non_dist_commitTxnTime;
        }
    }
}