using MathNet.Numerics.Statistics;
using System;
using System.Collections.Generic;
using System.IO;
using Utilities;

namespace SnapperExperimentProcess
{
    public class ExperimentResultAggregator
    {
        readonly WorkloadConfiguration workload;
        readonly string filePath;
        StreamWriter file;
        WorkloadResult[,] results;
        readonly int[] percentilesToCalculate;

        public ExperimentResultAggregator(WorkloadConfiguration workload)
        {
            this.workload = workload;
            results = new WorkloadResult[Constants.numEpoch, Constants.numWorker];
            percentilesToCalculate = new int[] { 25, 50, 75, 90, 99 };
            filePath = Constants.resultPath;
        }

        public void SetResult(int i, int j, WorkloadResult result)
        {
            results[i, j] = result;
        }

        public static WorkloadResult AggregateResultForEpoch(WorkloadResult[] result)
        {
            var aggResult = result[0];
            for (int i = 1; i < result.Length; i++) aggResult.MergeData(result[i]);
            return aggResult;
        }

        public bool AggregateResultsAndPrint()
        {
            Console.WriteLine("Aggregating results and printing");

            var pact_dist_tp = new List<double>();
            var pact_non_dist_tp = new List<double>();

            var act_dist_tp = new List<double>();
            var act_dist_abort = new List<double>();
            var act_dist_abort_deadlock = new List<double>();
            var act_dist_abort_notSerializable = new List<double>();
            var act_dist_abort_notSureSerializable = new List<double>();

            var act_non_dist_tp = new List<double>();
            var act_non_dist_abort = new List<double>();
            var act_non_dist_abort_deadlock = new List<double>();
            var act_non_dist_abort_notSerializable = new List<double>();
            var act_non_dist_abort_notSureSerializable = new List<double>();

            // latencies
            var pact_dist_latencies = new BasicLatencyInfo();
            var pact_non_dist_latencies = new BasicLatencyInfo();

            var act_dist_latencies = new BasicLatencyInfo();
            var act_non_dist_latencies = new BasicLatencyInfo();

            for (int e = Constants.numWarmupEpoch; e < Constants.numEpoch; e++) 
            {
                var result = new WorkloadResult[Constants.numWorker];
                for (int i = 0; i < Constants.numWorker; i++) result[i] = results[e, i];
                var aggResultForOneEpoch = AggregateResultForEpoch(result);

                pact_dist_latencies.MergeData(aggResultForOneEpoch.pact_dist_result.latencies);
                pact_non_dist_latencies.MergeData(aggResultForOneEpoch.pact_non_dist_result.latencies);
                act_dist_latencies.MergeData(aggResultForOneEpoch.act_dist_result.basic_result.latencies);
                act_non_dist_latencies.MergeData(aggResultForOneEpoch.act_non_dist_result.basic_result.latencies);

                var data = aggResultForOneEpoch.GetPrintData();
                pact_dist_tp.Add(data.pact_dist_print.throughput);
                pact_non_dist_tp.Add(data.pact_non_dist_print.throughput);

                act_dist_tp.Add(data.act_dist_print.basic_data.throughput);
                act_dist_abort.Add(data.act_dist_print.abort);
                act_dist_abort_deadlock.Add(data.act_dist_print.abort_deadlock);
                act_dist_abort_notSerializable.Add(data.act_dist_print.abort_notSerializable);
                act_dist_abort_notSureSerializable.Add(data.act_dist_print.abort_notSureSerializable);

                act_non_dist_tp.Add(data.act_non_dist_print.basic_data.throughput);
                act_non_dist_abort.Add(data.act_non_dist_print.abort);
                act_non_dist_abort_deadlock.Add(data.act_non_dist_print.abort_deadlock);
                act_non_dist_abort_notSerializable.Add(data.act_non_dist_print.abort_notSerializable);
                act_non_dist_abort_notSureSerializable.Add(data.act_non_dist_print.abort_notSureSerializable);
            }

            if (pact_dist_tp.Count != 0 && pact_dist_tp.StandardDeviation() > pact_dist_tp.Mean() * Constants.sdSafeRange)
            {
                Console.WriteLine($"pact_dist_tp sd out of range: mean = {pact_dist_tp.Mean()}, sd = {pact_dist_tp.StandardDeviation()}");
                return false;
            }
            if (pact_non_dist_tp.Count != 0 && pact_non_dist_tp.StandardDeviation() > pact_non_dist_tp.Mean() * Constants.sdSafeRange)
            {
                Console.WriteLine($"pact_non_dist_tp sd out of range: mean = {pact_non_dist_tp.Mean()}, sd = {pact_non_dist_tp.StandardDeviation()}");
                return false;
            }
            if (act_dist_tp.Count != 0 && act_dist_tp.StandardDeviation() > act_dist_tp.Mean() * Constants.sdSafeRange)
            {
                Console.WriteLine($"act_dist_tp sd out of range: mean = {act_dist_tp.Mean()}, sd = {act_dist_tp.StandardDeviation()}");
                return false;
            }
            if (act_non_dist_tp.Count != 0 && act_non_dist_tp.StandardDeviation() > act_non_dist_tp.Mean() * Constants.sdSafeRange)
            {
                Console.WriteLine($"act_non_dist_tp sd out of range: mean = {act_non_dist_tp.Mean()}, sd = {act_non_dist_tp.StandardDeviation()}");
                return false;
            }

            using (file = new StreamWriter(filePath, true))
            {
                file.Write($"{workload.pactPipeSize} {workload.actPipeSize} " +
                           $"{Helper.ChangeFormat(workload.grainSkewness * 100, 1)}% " + 
                           $"{workload.pactPercent}% {workload.distPercent}% ");

                if (workload.pactPercent > 0)
                {
                    file.Write($"{Helper.ChangeFormat(pact_dist_tp.Mean(), 0)} {Helper.ChangeFormat(pact_dist_tp.StandardDeviation(), 0)} ");
                    file.Write($"{Helper.ChangeFormat(pact_non_dist_tp.Mean(), 0)} {Helper.ChangeFormat(pact_non_dist_tp.StandardDeviation(), 0)} ");
                }

                if (workload.pactPercent < 100)
                {
                    file.Write($"{Helper.ChangeFormat(act_dist_tp.Mean(), 0)} {Helper.ChangeFormat(act_dist_tp.StandardDeviation(), 0)} ");
                    file.Write($"{Helper.ChangeFormat(act_non_dist_tp.Mean(), 0)} {Helper.ChangeFormat(act_non_dist_tp.StandardDeviation(), 0)} ");

                    var act_dist_abort_rw = 100.0 - act_dist_abort_deadlock.Mean() - act_dist_abort_notSerializable.Mean() - act_dist_abort_notSureSerializable.Mean();
                    file.Write($"{Helper.ChangeFormat(act_dist_abort.Mean(), 2)}% " +
                               $"{Helper.ChangeFormat(act_dist_abort_rw, 2)}% " +
                               $"{Helper.ChangeFormat(act_dist_abort_deadlock.Mean(), 2)}% " +
                               $"{Helper.ChangeFormat(act_dist_abort_notSerializable.Mean(), 2)}% " +
                               $"{Helper.ChangeFormat(act_dist_abort_notSureSerializable.Mean(), 2)}% ");

                    var act_non_dist_abort_rw = 100.0 - act_non_dist_abort_deadlock.Mean() - act_non_dist_abort_notSerializable.Mean() - act_non_dist_abort_notSureSerializable.Mean();
                    file.Write($"{Helper.ChangeFormat(act_non_dist_abort.Mean(), 2)}% " +
                               $"{Helper.ChangeFormat(act_non_dist_abort_rw, 2)}% " +
                               $"{Helper.ChangeFormat(act_non_dist_abort_deadlock.Mean(), 2)}% " +
                               $"{Helper.ChangeFormat(act_non_dist_abort_notSerializable.Mean(), 2)}% " +
                               $"{Helper.ChangeFormat(act_non_dist_abort_notSureSerializable.Mean(), 2)}% ");
                }

                if (workload.pactPercent > 0)
                {
                    foreach (var percentile in percentilesToCalculate)
                    {
                        var latency = ArrayStatistics.PercentileInplace(pact_dist_latencies.latency.ToArray(), percentile);
                        file.Write($"{Helper.ChangeFormat(latency, 1)} ");
                    }

                    file.Write($"{Helper.ChangeFormat(pact_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                    file.Write($"{Helper.ChangeFormat(pact_dist_latencies.executeTxnTime.Mean(), 1)} ");
                    file.Write($"{Helper.ChangeFormat(pact_dist_latencies.commitTxnTime.Mean(), 1)} ");

                    foreach (var percentile in percentilesToCalculate)
                    {
                        var latency = ArrayStatistics.PercentileInplace(pact_non_dist_latencies.latency.ToArray(), percentile);
                        file.Write($"{Helper.ChangeFormat(latency, 1)} ");
                    }

                    file.Write($"{Helper.ChangeFormat(pact_non_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                    file.Write($"{Helper.ChangeFormat(pact_non_dist_latencies.executeTxnTime.Mean(), 1)} ");
                    file.Write($"{Helper.ChangeFormat(pact_non_dist_latencies.commitTxnTime.Mean(), 1)} ");
                }

                if (workload.pactPercent < 100)
                {
                    foreach (var percentile in percentilesToCalculate)
                    {
                        var latency = ArrayStatistics.PercentileInplace(act_dist_latencies.latency.ToArray(), percentile);
                        file.Write($"{Helper.ChangeFormat(latency, 1)} ");
                    }

                    file.Write($"{Helper.ChangeFormat(act_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                    file.Write($"{Helper.ChangeFormat(act_dist_latencies.executeTxnTime.Mean(), 1)} ");
                    file.Write($"{Helper.ChangeFormat(act_dist_latencies.commitTxnTime.Mean(), 1)} ");

                    foreach (var percentile in percentilesToCalculate)
                    {
                        var latency = ArrayStatistics.PercentileInplace(act_non_dist_latencies.latency.ToArray(), percentile);
                        file.Write($"{Helper.ChangeFormat(latency, 1)} ");
                    }

                    file.Write($"{Helper.ChangeFormat(act_non_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                    file.Write($"{Helper.ChangeFormat(act_non_dist_latencies.executeTxnTime.Mean(), 1)} ");
                    file.Write($"{Helper.ChangeFormat(act_non_dist_latencies.commitTxnTime.Mean(), 1)} ");
                }
                
                file.WriteLine();
            }
            return true;
        }
    }
}