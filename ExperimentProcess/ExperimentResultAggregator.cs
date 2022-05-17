using MathNet.Numerics.Statistics;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Utilities;

namespace ExperimentProcess
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
            Trace.Assert(result.Length >= 1);
            int aggNumDetCommitted = result[0].numDetCommitted;
            int aggNumNonDetCommitted = result[0].numNonDetCommitted;
            int aggNumDetTransactions = result[0].numDetTxn;
            int aggNumNonDetTransactions = result[0].numNonDetTxn;
            int aggNumNotSerializable = result[0].numNotSerializable;
            int aggNumNotSureSerializable = result[0].numNotSerializable;
            int aggNumDeadlock = result[0].numDeadlock;
            long aggStartTime = result[0].startTime;
            long aggEndTime = result[0].endTime;
            var aggLatencies = new List<double>();
            var aggDetLatencies = new List<double>();
            aggLatencies.AddRange(result[0].latencies);
            aggDetLatencies.AddRange(result[0].det_latencies);

            for (int i = 1; i < result.Length; i++)    // reach thread has a result
            {
                aggNumDetCommitted += result[i].numDetCommitted;
                aggNumNonDetCommitted += result[i].numNonDetCommitted;
                aggNumDetTransactions += result[i].numDetTxn;
                aggNumNonDetTransactions += result[i].numNonDetTxn;
                aggNumNotSerializable += result[i].numNotSerializable;
                aggNumNotSureSerializable += result[i].numNotSureSerializable;
                aggNumDeadlock += result[i].numDeadlock;
                aggStartTime = (result[i].startTime < aggStartTime) ? result[i].startTime : aggStartTime;
                aggEndTime = (result[i].endTime < aggEndTime) ? result[i].endTime : aggEndTime;
                aggLatencies.AddRange(result[i].latencies);
                aggDetLatencies.AddRange(result[i].det_latencies);
            }
            var res = new WorkloadResult(aggNumDetTransactions, aggNumNonDetTransactions, aggNumDetCommitted, aggNumNonDetCommitted, aggStartTime, aggEndTime, aggNumNotSerializable, aggNumNotSureSerializable, aggNumDeadlock);
            res.setLatency(aggLatencies, aggDetLatencies);
            return res;
        }

        public void AggregateResultsAndPrint()
        {
            Console.WriteLine("Aggregating results and printing");

            var detThroughPutAccumulator = new List<float>();
            var nonDetThroughPutAccumulator = new List<float>();
            var abortRateAccumulator = new List<double>();
            var notSerializableRateAccumulator = new List<float>();
            var notSureSerializableRateAccumulator = new List<float>();
            var deadlockRateAccumulator = new List<float>();
            var ioThroughputAccumulator = new List<float>();

            //Skip the epochs upto warm up epochs
            WorkloadResult aggResult = null;
            for (int epochNumber = numWarmupEpoch; epochNumber < numEpochs; epochNumber++)
            {
                if (Constants.numWorker == 1) aggResult = results[epochNumber, 0];
                else
                {
                    var result = new WorkloadResult[Constants.numWorker];
                    for (int i = 0; i < Constants.numWorker; i++) result[i] = results[epochNumber, i];
                    aggResult = AggregateResultForEpoch(result);
                }
                
                var time = aggResult.endTime - aggResult.startTime;
                float detCommittedTxnThroughput = (float)aggResult.numDetCommitted * 1000 / time;  // the throughput only include committed transactions
                float nonDetCommittedTxnThroughput = (float)aggResult.numNonDetCommitted * 1000 / time;
                double abortRate = 0;
                var numAbort = aggResult.numNonDetTxn - aggResult.numNonDetCommitted;
                if (pactPercent < 100)
                {
                    abortRate = numAbort * 100.0 / aggResult.numNonDetTxn;    // the abort rate is based on all non-det txns
                    if (numAbort > 0)
                    {
                        var notSerializable = aggResult.numNotSerializable * 100.0 / numAbort;   // number of transactions abort due to not serializable among all aborted transactions
                        notSerializableRateAccumulator.Add((float)notSerializable);
                        var notSureSerializable = aggResult.numNotSureSerializable * 100.0 / numAbort;   // abort due to incomplete AfterSet
                        notSureSerializableRateAccumulator.Add((float)notSureSerializable);
                        var deadlock = aggResult.numDeadlock * 100.0 / numAbort;
                        deadlockRateAccumulator.Add((float)deadlock);
                    }
                }
                detThroughPutAccumulator.Add(detCommittedTxnThroughput);
                nonDetThroughPutAccumulator.Add(nonDetCommittedTxnThroughput);
                abortRateAccumulator.Add(abortRate);

                ioThroughputAccumulator.Add((float)IOCount[epochNumber] * 1000 / time);
            }

            //Compute statistics on the accumulators, maybe a better way is to maintain a sorted list
            var detThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(detThroughPutAccumulator.ToArray());
            var nonDetThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(nonDetThroughPutAccumulator.ToArray());
            var abortRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(abortRateAccumulator.ToArray());
            var notSerializableRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(notSerializableRateAccumulator.ToArray());
            var notSureSerializableRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(notSureSerializableRateAccumulator.ToArray());
            var deadlockRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(deadlockRateAccumulator.ToArray());
            var ioThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(ioThroughputAccumulator.ToArray());
            using (file = new StreamWriter(filePath, true))
            {
                //file.Write($"numWarehouse={Constants.NUM_W_PER_SILO} siloCPU={Constants.numCPUPerSilo} distribution={workload.distribution} benchmark={workload.benchmark} ");
                //file.Write($"{workload.pactPercent}% ");
                if (pactPercent > 0) file.Write($"{detThroughputMeanAndSd.Item1:0} {detThroughputMeanAndSd.Item2:0} ");
                if (pactPercent < 100)
                {
                    file.Write($"{nonDetThroughputMeanAndSd.Item1:0} {nonDetThroughputMeanAndSd.Item2:0} ");
                    file.Write($"{Math.Round(abortRateMeanAndSd.Item1, 2).ToString().Replace(',', '.')}% ");
                    if (pactPercent > 0)
                    {
                        var abortRWConflict = 100 - deadlockRateMeanAndSd.Item1 - notSerializableRateMeanAndSd.Item1 - notSureSerializableRateMeanAndSd.Item1;
                        file.Write($"{Math.Round(abortRWConflict, 2).ToString().Replace(',', '.')}% {Math.Round(deadlockRateMeanAndSd.Item1, 2).ToString().Replace(',', '.')}% {Math.Round(notSerializableRateMeanAndSd.Item1, 2).ToString().Replace(',', '.')}% {Math.Round(notSureSerializableRateMeanAndSd.Item1, 2).ToString().Replace(',', '.')}% ");
                    }
                }
                if (Constants.implementationType == ImplementationType.SNAPPER)
                {
                    //file.Write($"{ioThroughputMeanAndSd.Item1} {ioThroughputMeanAndSd.Item2} ");   // number of IOs per second
                }
                if (pactPercent > 0)
                {
                    foreach (var percentile in percentilesToCalculate)
                    {
                        var lat = ArrayStatistics.PercentileInplace(aggResult.det_latencies.ToArray(), percentile);
                        file.Write($"{Math.Round(lat, 2).ToString().Replace(',', '.')} ");
                    }
                }
                if (pactPercent < 100)
                {
                    foreach (var percentile in percentilesToCalculate)
                    {
                        var lat = ArrayStatistics.PercentileInplace(aggResult.latencies.ToArray(), percentile);
                        file.Write($"{Math.Round(lat, 2).ToString().Replace(',', '.')} ");
                    }
                }
                file.WriteLine();
            }
            /*
            if (workload.pactPercent == 100) filePath = Constants.dataPath + $"PACT_{workload.numAccountsMultiTransfer}.txt";
            if (workload.pactPercent == 0)
            {
                if (nonDetCCType == ConcurrencyType.TIMESTAMP) filePath = Constants.dataPath + $"TS_{workload.numAccountsMultiTransfer}.txt";
                if (nonDetCCType == ConcurrencyType.S2PL) filePath = Constants.dataPath + $"2PL_{workload.numAccountsMultiTransfer}.txt";
            } 
            using (file = new System.IO.StreamWriter(filePath, true))
            {
                Console.WriteLine($"aggLatencies.count = {aggLatencies.Count}, aggDetLatencies.count = {aggDetLatencies.Count}");
                foreach (var latency in aggLatencies) file.WriteLine(latency);
                foreach (var latency in aggDetLatencies) file.WriteLine(latency);
            }*/
        }
    }
}