using System;
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
    public class BasicLatencyInfo
    {
        [Key(0)]
        public List<double> latency;
        [Key(1)]
        public List<double> prepareTxnTime;
        [Key(2)]
        public List<double> executeTxnTime;
        [Key(3)]
        public List<double> commitTxnTime;

        public BasicLatencyInfo()
        {
            latency = new List<double>();
            prepareTxnTime = new List<double>();
            executeTxnTime = new List<double>();
            commitTxnTime = new List<double>();
        }

        public void SetLatency(List<double> latency, List<double> prepareTxnTime, List<double> executeTxnTime, List<double> commitTxnTime)
        {
            this.latency = latency;
            this.prepareTxnTime = prepareTxnTime;
            this.executeTxnTime = executeTxnTime;
            this.commitTxnTime = commitTxnTime;
        }

        public void MergeData(BasicLatencyInfo latencies)
        {
            latency.AddRange(latencies.latency);
            prepareTxnTime.AddRange(latencies.prepareTxnTime);
            executeTxnTime.AddRange(latencies.executeTxnTime);
            commitTxnTime.AddRange(latencies.commitTxnTime);
        }
    }

    [MessagePackObject]
    public class BasicResult
    {
        [Key(0)]
        public int numCommit;
        [Key(1)]
        public BasicLatencyInfo latencies;

        public BasicResult()
        {
            latencies = new BasicLatencyInfo();
        }

        public BasicResult(int numCommit)
        {
            this.numCommit = numCommit;
            latencies = new BasicLatencyInfo();
        }

        public void MergeData(BasicResult res)
        {
            numCommit += res.numCommit;
            latencies.MergeData(res.latencies);
        }
    }

    [MessagePackObject]
    public class BasicACTResult
    {
        [Key(0)]
        public BasicResult basic_result;
        [Key(1)]
        public int numEmit;
        [Key(2)]
        public int numDeadlock;
        [Key(3)]
        public int numNotSerializable;
        [Key(4)]
        public int numNotSureSerializable;

        public BasicACTResult()
        {
            basic_result = new BasicResult();
        }

        public BasicACTResult(int numCommit, int numEmit, int numDeadlock, int numNotSerializable, int numNotSureSerializable)
        {
            basic_result = new BasicResult(numCommit);
            this.numEmit = numEmit;
            this.numDeadlock = numDeadlock;
            this.numNotSerializable = numNotSerializable;
            this.numNotSureSerializable = numNotSureSerializable;
        }

        public void MergeData(BasicACTResult res)
        {
            basic_result.MergeData(res.basic_result);
            numEmit += res.numEmit;
            numDeadlock += res.numDeadlock;
            numNotSerializable = res.numNotSerializable;
            numNotSureSerializable = res.numNotSureSerializable;
        }
    }

    [MessagePackObject]
    public class WorkloadResult
    {
        [Key(0)]
        public long startTime;
        [Key(1)]
        public long endTime;

        [Key(2)]
        public BasicResult pact_dist_result;
        [Key(3)]
        public BasicResult pact_non_dist_result;

        [Key(4)]
        public BasicACTResult act_dist_result;
        [Key(5)]
        public BasicACTResult act_non_dist_result;

        public WorkloadResult()
        {
            pact_dist_result = new BasicResult();
            pact_non_dist_result = new BasicResult();
            act_dist_result = new BasicACTResult();
            act_non_dist_result = new BasicACTResult();
        }

        public void SetTime(long startTime, long endTime)
        {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public void SetNumber(bool isDet, bool isDist, int numCommit, int numEmit, int numDeadlock, int numNotSerializable, int numNotSureSerializable)
        {
            if (isDet)
            {
                if (isDist) pact_dist_result = new BasicResult(numCommit);
                else pact_non_dist_result = new BasicResult(numCommit);
            }
            else
            {
                if (isDist) act_dist_result = new BasicACTResult(numCommit, numEmit, numDeadlock, numNotSerializable, numNotSureSerializable);
                else act_non_dist_result = new BasicACTResult(numCommit, numEmit, numDeadlock, numNotSerializable, numNotSureSerializable);
            }
        }

        public void SetLatency(bool isDet, bool isDist, List<double> latency, List<double> prepareTxnTime, List<double> executeTxnTime, List<double> commitTxnTime)
        {
            if (isDet)
            {
                if (isDist) pact_dist_result.latencies.SetLatency(latency, prepareTxnTime, executeTxnTime, commitTxnTime);
                else pact_non_dist_result.latencies.SetLatency(latency, prepareTxnTime, executeTxnTime, commitTxnTime);
            }
            else
            {
                if (isDist) act_dist_result.basic_result.latencies.SetLatency(latency, prepareTxnTime, executeTxnTime, commitTxnTime);
                else act_non_dist_result.basic_result.latencies.SetLatency(latency, prepareTxnTime, executeTxnTime, commitTxnTime);
            }
        }

        public void MergeData(WorkloadResult res)
        {
            startTime = Math.Min(startTime, res.startTime);
            endTime = Math.Max(endTime, res.endTime);

            pact_dist_result.MergeData(res.pact_dist_result);
            pact_non_dist_result.MergeData(res.pact_non_dist_result);

            act_dist_result.MergeData(res.act_dist_result);
            act_non_dist_result.MergeData(res.act_non_dist_result);
        }

        public PrintData GetPrintData()
        {
            var data = new PrintData();

            data.pact_dist_print.Calculate(pact_dist_result.numCommit, startTime, endTime);
            data.pact_non_dist_print.Calculate(pact_non_dist_result.numCommit, startTime, endTime);

            data.act_dist_print.Calculate(act_dist_result.basic_result.numCommit, startTime, endTime, 
                act_dist_result.numEmit, act_dist_result.numDeadlock, act_dist_result.numNotSerializable, act_dist_result.numNotSureSerializable);
            data.act_non_dist_print.Calculate(act_non_dist_result.basic_result.numCommit, startTime, endTime,
                act_non_dist_result.numEmit, act_non_dist_result.numDeadlock, act_non_dist_result.numNotSerializable, act_non_dist_result.numNotSureSerializable);
            return data;
        }
    }

    public class BasicPrintData
    {
        public long throughput;

        public void Calculate(int numCommit, long startTime, long endTime)
        {
            throughput = numCommit * 1000 / (endTime - startTime);
        }
    }

    public class BasicACTPrintData
    {
        public BasicPrintData basic_data;
        public double abort;
        public double abort_deadlock;
        public double abort_notSerializable;
        public double abort_notSureSerializable;

        public BasicACTPrintData()
        {
            basic_data = new BasicPrintData();
        }

        public void Calculate(int numCommit, long startTime, long endTime, int numEmit, int numDeadlock, int numNotSerializable, int numNotSureSerializable)
        {
            basic_data.Calculate(numCommit, startTime, endTime);
            var numAbort = numEmit - numCommit;
            abort = numAbort * 100.0 / numEmit;
            abort_deadlock = numDeadlock * 100.0 / numAbort;
            abort_notSerializable = numNotSerializable * 100.0 / numAbort;
            abort_notSureSerializable = numNotSureSerializable * 100.0 / numAbort;
        }
    }

    public class PrintData
    {
        public BasicPrintData pact_dist_print;
        public BasicPrintData pact_non_dist_print;

        public BasicACTPrintData act_dist_print;
        public BasicACTPrintData act_non_dist_print;

        public PrintData()
        {
            pact_dist_print = new BasicPrintData();
            pact_non_dist_print = new BasicPrintData();
            act_dist_print = new BasicACTPrintData();
            act_non_dist_print = new BasicACTPrintData();
        }
    }
}