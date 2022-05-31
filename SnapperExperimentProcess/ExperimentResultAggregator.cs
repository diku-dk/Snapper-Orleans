using MathNet.Numerics.Statistics;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Utilities;

namespace SnapperExperimentProcess
{
    public class ExperimentResultAggregator
    {
        int pactPercent;
        int numEpochs;
        int numWarmupEpoch;
        readonly long[] IOCount;

        string filePath;
        StreamWriter file;
        WorkloadResult[,] results;
        int[] percentilesToCalculate;

        public ExperimentResultAggregator()
        {
        }

        public ExperimentResultAggregator(int pactPercent, int numEpochs, int numWarmupEpoch, long[] IOCount)
        {
            Debug.Assert(numEpochs >= 1);
            Debug.Assert(Constants.numWorker >= 1);

            this.pactPercent = pactPercent;
            this.numEpochs = numEpochs;
            this.numWarmupEpoch = numWarmupEpoch;
            this.IOCount = IOCount;
            results = new WorkloadResult[numEpochs, Constants.numWorker];
            percentilesToCalculate = new int[] { 25, 50, 75, 90, 99 };
            filePath = Constants.resultPath;
        }

        public void SetResult(int i, int j, WorkloadResult result)
        {
            results[i, j] = result;
        }

        public static WorkloadResult AggregateResultForEpoch(WorkloadResult[] result)
        {
            Debug.Assert(result.Length == Constants.numWorker);
            var aggResult = result[0];
            for (int i = 1; i < result.Length; i++) aggResult.MergeData(result[i]);
            return aggResult;
        }

        public void AggregateResultsAndPrint()
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

            for (int e = numWarmupEpoch; e < numEpochs; e++) 
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

            var pact_dist_tp_meanAndSd = ArrayStatistics.MeanStandardDeviation(pact_dist_tp.ToArray());
            var pact_non_dist_tp_meanAndSd = ArrayStatistics.MeanStandardDeviation(pact_non_dist_tp.ToArray());

            var act_dist_tp_meanAndSd = ArrayStatistics.MeanStandardDeviation(act_dist_tp.ToArray());
            var act_non_dist_tp_meanAndSd = ArrayStatistics.MeanStandardDeviation(act_non_dist_tp.ToArray());

            using (file = new StreamWriter(filePath, true))
            {
                file.Write($"{ChangeFormat(pact_dist_tp_meanAndSd.Mean, 0)} {ChangeFormat(pact_dist_tp_meanAndSd.StandardDeviation, 0)} ");
                file.Write($"{ChangeFormat(pact_non_dist_tp_meanAndSd.Mean, 0)} {ChangeFormat(pact_non_dist_tp_meanAndSd.StandardDeviation, 0)} ");

                file.Write($"{ChangeFormat(act_dist_tp_meanAndSd.Mean, 0)} {ChangeFormat(act_dist_tp_meanAndSd.StandardDeviation, 0)} ");
                file.Write($"{ChangeFormat(act_non_dist_tp_meanAndSd.Mean, 0)} {ChangeFormat(act_non_dist_tp_meanAndSd.StandardDeviation, 0)} ");

                var act_dist_abort_rw = 100.0 - act_dist_abort_deadlock.Mean() - act_dist_abort_notSerializable.Mean() - act_dist_abort_notSureSerializable.Mean();
                file.Write($"{ChangeFormat(act_dist_abort.Mean(), 2)}% " +
                           $"{ChangeFormat(act_dist_abort_rw, 2)}% " +
                           $"{ChangeFormat(act_dist_abort_deadlock.Mean(), 2)}% " +
                           $"{ChangeFormat(act_dist_abort_notSerializable.Mean(), 2)}% " +
                           $"{ChangeFormat(act_dist_abort_notSureSerializable.Mean(), 2)}% ");

                var act_non_dist_abort_rw = 100.0 - act_non_dist_abort_deadlock.Mean() - act_non_dist_abort_notSerializable.Mean() - act_non_dist_abort_notSureSerializable.Mean();
                file.Write($"{ChangeFormat(act_non_dist_abort.Mean(), 2)}% " +
                           $"{ChangeFormat(act_non_dist_abort_rw, 2)}% " +
                           $"{ChangeFormat(act_non_dist_abort_deadlock.Mean(), 2)}% " +
                           $"{ChangeFormat(act_non_dist_abort_notSerializable.Mean(), 2)}% " +
                           $"{ChangeFormat(act_non_dist_abort_notSureSerializable.Mean(), 2)}% ");

                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(pact_dist_latencies.latency.ToArray(), percentile);
                    file.Write($"{ChangeFormat(latency, 1)} ");
                }

                file.Write($"{ChangeFormat(pact_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                file.Write($"{ChangeFormat(pact_dist_latencies.executeTxnTime.Mean(), 1)} ");
                file.Write($"{ChangeFormat(pact_dist_latencies.commitTxnTime.Mean(), 1)} ");

                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(pact_non_dist_latencies.latency.ToArray(), percentile);
                    file.Write($"{ChangeFormat(latency, 1)} ");
                }

                file.Write($"{ChangeFormat(pact_non_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                file.Write($"{ChangeFormat(pact_non_dist_latencies.executeTxnTime.Mean(), 1)} ");
                file.Write($"{ChangeFormat(pact_non_dist_latencies.commitTxnTime.Mean(), 1)} ");

                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(act_dist_latencies.latency.ToArray(), percentile);
                    file.Write($"{ChangeFormat(latency, 1)} ");
                }

                file.Write($"{ChangeFormat(act_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                file.Write($"{ChangeFormat(act_dist_latencies.executeTxnTime.Mean(), 1)} ");
                file.Write($"{ChangeFormat(act_dist_latencies.commitTxnTime.Mean(), 1)} ");

                foreach (var percentile in percentilesToCalculate)
                {
                    var latency = ArrayStatistics.PercentileInplace(act_non_dist_latencies.latency.ToArray(), percentile);
                    file.Write($"{ChangeFormat(latency, 1)} ");
                }

                file.Write($"{ChangeFormat(act_non_dist_latencies.prepareTxnTime.Mean(), 1)} ");
                file.Write($"{ChangeFormat(act_non_dist_latencies.executeTxnTime.Mean(), 1)} ");
                file.Write($"{ChangeFormat(act_non_dist_latencies.commitTxnTime.Mean(), 1)} ");



                file.WriteLine();
            }
        }

        static string ChangeFormat(double n, int num)
        {
            return Math.Round(n, num).ToString().Replace(',', '.');
        }
    }
}