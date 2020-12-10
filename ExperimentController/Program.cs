using NetMQ;
using System;
using Orleans;
using Utilities;
using System.Linq;
using NetMQ.Sockets;
using System.Threading;
using ExperimentProcess;
using System.Diagnostics;
using System.Configuration;
using SmallBank.Interfaces;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Statistics;
using Concurrency.Interface.Logging;
using System.Collections.Specialized;
using Concurrency.Interface.Nondeterministic;

namespace ExperimentController
{
    class Program
    {
        static string sinkAddress;
        static string workerAddress;
        static int numWorkerNodes;
        static int numWarmupEpoch;
        static int batchInterval;
        static IClusterClient client;
        static volatile bool asyncInitializationDone = false;
        static volatile bool loadingDone = false;
        static CountdownEvent ackedWorkers;
        static WorkloadConfiguration workload;
        static ExecutionGrainConfiguration exeConfig;
        static CoordinatorGrainConfiguration coordConfig;
        static WorkloadResults[,] results;
        static string filePath;
        static System.IO.StreamWriter file;
        static ConcurrencyType nonDetCCType;
        static int numCoordinators;
        static ISerializer serializer;

        // to count num_rpc
        static bool taskDone = false;
        static long[] txnIntraCount;
        static long[] txnInterCount;
        static long[] batchIntraCount;
        static long[] batchInterCount;
        static long[] tokenIntraCount;
        static long[] tokenInterCount;
        static IGlobalTransactionCoordinatorGrain[] coords;

        // to print grain data
        static bool finishPrint = false;
        static bool enableCounter = true;

        static int vCPU;

        private static void GenerateWorkLoadFromSettingsFile()
        {
            //Parse and initialize benchmarkframework section
            var benchmarkFrameWorkSection = ConfigurationManager.GetSection("BenchmarkFrameworkConfig") as NameValueCollection;
            workload.numWorkerNodes = int.Parse(benchmarkFrameWorkSection["numWorkerNodes"]);
            numWorkerNodes = workload.numWorkerNodes;
            workload.numConnToClusterPerWorkerNode = int.Parse(benchmarkFrameWorkSection["numConnToClusterPerWorkerNode"]);
            workload.numThreadsPerWorkerNode = int.Parse(benchmarkFrameWorkSection["numThreadsPerWorkerNode"]);
            workload.epochDurationMSecs = int.Parse(benchmarkFrameWorkSection["epochDurationMSecs"]);
            workload.numEpochs = int.Parse(benchmarkFrameWorkSection["numEpochs"]);
            numWarmupEpoch = int.Parse(benchmarkFrameWorkSection["numWarmupEpoch"]);
            workload.asyncMsgLengthPerThread = int.Parse(benchmarkFrameWorkSection["asyncMsgLengthPerThread"]);
            workload.percentilesToCalculate = Array.ConvertAll(benchmarkFrameWorkSection["percentilesToCalculate"].Split(","), x => int.Parse(x));

            //Parse Snapper configuration
            var snapperConfigSection = ConfigurationManager.GetSection("SnapperConfig") as NameValueCollection;
            nonDetCCType = Enum.Parse<ConcurrencyType>(snapperConfigSection["nonDetCCType"]);
            var maxNonDetWaitingLatencyInMSecs = int.Parse(snapperConfigSection["maxNonDetWaitingLatencyInMSecs"]);
            batchInterval = int.Parse(snapperConfigSection["batchIntervalMSecs"]);
            var idleIntervalTillBackOffSecs = int.Parse(snapperConfigSection["idleIntervalTillBackOffSecs"]);
            var backoffIntervalMsecs = int.Parse(snapperConfigSection["backoffIntervalMsecs"]);
            numCoordinators = int.Parse(snapperConfigSection["numCoordinators"]);
            var dataFormat = Enum.Parse<dataFormatType>(snapperConfigSection["dataFormat"]);
            var logStorage = Enum.Parse<StorageWrapperType>(snapperConfigSection["logStorage"]);

            //Parse workload specific configuration, assumes only one defined in file
            var benchmarkConfigSection = ConfigurationManager.GetSection("BenchmarkConfig") as NameValueCollection;
            workload.benchmark = Enum.Parse<BenchmarkType>(benchmarkConfigSection["benchmark"]);
            workload.distribution = Enum.Parse<Distribution>(benchmarkConfigSection["distribution"]);
            workload.zipfianConstant = float.Parse(benchmarkConfigSection["zipfianConstant"]);
            workload.deterministicTxnPercent = float.Parse(benchmarkConfigSection["deterministicTxnPercent"]);
            workload.mixture = Array.ConvertAll(benchmarkConfigSection["mixture"].Split(","), x => int.Parse(x));
            switch (workload.benchmark)
            {
                case BenchmarkType.SMALLBANK:
                    workload.numAccounts = int.Parse(benchmarkConfigSection["numAccounts"]);
                    workload.numAccountsPerGroup = int.Parse(benchmarkConfigSection["numAccountsPerGroup"]);
                    workload.numAccountsMultiTransfer = int.Parse(benchmarkConfigSection["numAccountsMultiTransfer"]);
                    workload.numGrainsMultiTransfer = int.Parse(benchmarkConfigSection["numGrainsMultiTransfer"]);
                    workload.grainImplementationType = Enum.Parse<ImplementationType>(benchmarkConfigSection["grainImplementationType"]);
                    exeConfig = new ExecutionGrainConfiguration("SmallBank.Grains.CustomerAccountGroupGrain", new LoggingConfiguration(dataFormat, logStorage), new ConcurrencyConfiguration(nonDetCCType));
                    break;
                default:
                    throw new Exception("Unknown benchmark type");
            }
            coordConfig = new CoordinatorGrainConfiguration(batchInterval, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numCoordinators);
            Console.WriteLine("Generated workload configuration");

        }

        private static void AggregateResultsAndPrint()
        {
            Trace.Assert(workload.numEpochs >= 1);
            Trace.Assert(numWorkerNodes >= 1);
            var aggLatencies = new List<double>();
            var aggNetworkTime = new List<double>();
            var aggEmitTime = new List<double>();
            var aggExecuteTime = new List<double>();
            var aggWaitBatchCommitTime = new List<double>();
            var aggDetLatencies = new List<double>();
            var aggDetNetworkTime = new List<double>();
            var aggDetEmitTime = new List<double>();
            var aggDetWaitBatchScheduleTime = new List<double>();
            var aggDetExecuteTime = new List<double>();
            var aggDetWaitBatchCommitTime = new List<double>();
            var txnIntraRPCThroughputAccumulator = new List<float>();
            var txnInterRPCThroughputAccumulator = new List<float>();
            var batchIntraRPCThroughputAccumulator = new List<float>();
            var batchInterRPCThroughputAccumulator = new List<float>();
            var tokenIntraRPCThroughputAccumulator = new List<float>();
            var tokenInterRPCThroughputAccumulator = new List<float>();
            var detThroughPutAccumulator = new List<float>();
            var nonDetThroughPutAccumulator = new List<float>();
            var abortRateAccumulator = new List<double>();
            var notSerializableRateAccumulator = new List<float>();
            var deadlockRateAccumulator = new List<float>();
            //Skip the epochs upto warm up epochs
            for (int epochNumber = numWarmupEpoch; epochNumber < workload.numEpochs; epochNumber++)
            {
                int aggNumDetCommitted = results[epochNumber, 0].numDetCommitted;
                int aggNumNonDetCommitted = results[epochNumber, 0].numNonDetCommitted;
                int aggNumDetTransactions = results[epochNumber, 0].numDetTxn;
                int aggNumNonDetTransactions = results[epochNumber, 0].numNonDetTxn;
                int aggNumNotSerializable = results[epochNumber, 0].numNotSerializable;
                int aggNumDeadlock = results[epochNumber, 0].numDeadlock;
                long aggStartTime = results[epochNumber, 0].startTime;
                long aggEndTime = results[epochNumber, 0].endTime;
                aggLatencies.AddRange(results[epochNumber, 0].latencies);
                aggNetworkTime.AddRange(results[epochNumber, 0].networkTime);
                aggEmitTime.AddRange(results[epochNumber, 0].emitTime);
                aggExecuteTime.AddRange(results[epochNumber, 0].executeTime);
                aggWaitBatchCommitTime.AddRange(results[epochNumber, 0].waitBatchCommitTime);
                aggDetLatencies.AddRange(results[epochNumber, 0].det_latencies);
                aggDetNetworkTime.AddRange(results[epochNumber, 0].det_networkTime);
                aggDetEmitTime.AddRange(results[epochNumber, 0].det_emitTime);
                aggDetWaitBatchScheduleTime.AddRange(results[epochNumber, 0].det_waitBatchScheduleTime);
                aggDetExecuteTime.AddRange(results[epochNumber, 0].det_executeTime);
                aggDetWaitBatchCommitTime.AddRange(results[epochNumber, 0].det_waitBatchCommitTime);
                for (int workerNode = 1; workerNode < numWorkerNodes; workerNode++)
                {
                    aggNumDetCommitted += results[epochNumber, workerNode].numDetCommitted;
                    aggNumNonDetCommitted += results[epochNumber, workerNode].numNonDetCommitted;
                    aggNumDetTransactions += results[epochNumber, workerNode].numDetTxn;
                    aggNumNonDetTransactions += results[epochNumber, workerNode].numNonDetTxn;
                    aggNumNotSerializable += results[epochNumber, workerNode].numNotSerializable;
                    aggNumDeadlock += results[epochNumber, workerNode].numDeadlock;
                    aggStartTime = (results[epochNumber, workerNode].startTime < aggStartTime) ? results[epochNumber, workerNode].startTime : aggStartTime;
                    aggEndTime = (results[epochNumber, workerNode].endTime < aggEndTime) ? results[epochNumber, workerNode].endTime : aggEndTime;
                    aggLatencies.AddRange(results[epochNumber, workerNode].latencies);
                    aggNetworkTime.AddRange(results[epochNumber, workerNode].networkTime);
                    aggEmitTime.AddRange(results[epochNumber, workerNode].emitTime);
                    aggExecuteTime.AddRange(results[epochNumber, workerNode].executeTime);
                    aggWaitBatchCommitTime.AddRange(results[epochNumber, workerNode].waitBatchCommitTime);
                    aggDetLatencies.AddRange(results[epochNumber, workerNode].det_latencies);
                    aggDetNetworkTime.AddRange(results[epochNumber, workerNode].det_networkTime);
                    aggDetEmitTime.AddRange(results[epochNumber, workerNode].det_emitTime);
                    aggDetWaitBatchScheduleTime.AddRange(results[epochNumber, workerNode].det_waitBatchScheduleTime);
                    aggDetExecuteTime.AddRange(results[epochNumber, workerNode].det_executeTime);
                    aggDetWaitBatchCommitTime.AddRange(results[epochNumber, workerNode].det_waitBatchCommitTime);
                }
                var time = aggEndTime - aggStartTime;
                float txnIntraRPCThroughput = txnIntraCount[epochNumber] * 1000 / time;
                float txnInterRPCThroughput = txnInterCount[epochNumber] * 1000 / time;
                float batchIntraRPCThroughput = batchIntraCount[epochNumber] * 1000 / time;
                float batchInterRPCThroughput = batchInterCount[epochNumber] * 1000 / time;
                float tokenIntraRPCThroughput = tokenIntraCount[epochNumber] * 1000 / time;
                float tokenInterRPCThroughput = tokenInterCount[epochNumber] * 1000 / time;
                float detCommittedTxnThroughput = (float)aggNumDetCommitted * 1000 / time;  // the throughput only include committed transactions
                float nonDetCommittedTxnThroughput = (float)aggNumNonDetCommitted * 1000 / time;
                double abortRate = 0;
                var numAbort = aggNumNonDetTransactions - aggNumNonDetCommitted;
                if (workload.deterministicTxnPercent < 100)
                {
                    abortRate = numAbort * 100.0 / aggNumNonDetTransactions;    // the abort rate is based on all non-det txns
                    if (numAbort > 0)
                    {
                        var notSerializable = aggNumNotSerializable * 100.0 / numAbort;   // number of transactions abort due to not serializable among all aborted transactions
                        notSerializableRateAccumulator.Add((float)notSerializable);
                        var deadlock = aggNumDeadlock * 100.0 / numAbort;
                        deadlockRateAccumulator.Add((float)deadlock);
                    }
                }
                txnIntraRPCThroughputAccumulator.Add(txnIntraRPCThroughput);
                txnInterRPCThroughputAccumulator.Add(txnInterRPCThroughput);
                batchIntraRPCThroughputAccumulator.Add(batchIntraRPCThroughput);
                batchInterRPCThroughputAccumulator.Add(batchInterRPCThroughput);
                tokenIntraRPCThroughputAccumulator.Add(tokenIntraRPCThroughput);
                tokenInterRPCThroughputAccumulator.Add(tokenInterRPCThroughput);
                detThroughPutAccumulator.Add(detCommittedTxnThroughput);
                nonDetThroughPutAccumulator.Add(nonDetCommittedTxnThroughput);
                abortRateAccumulator.Add(abortRate);
            }
            //Compute statistics on the accumulators, maybe a better way is to maintain a sorted list
            var networkTime = aggNetworkTime.Count > 0 ? aggNetworkTime.Average() : 0;
            var emitTime = aggEmitTime.Count > 0 ? aggEmitTime.Average() : 0;
            var executeTime = aggExecuteTime.Count > 0 ? aggExecuteTime.Average() : 0;
            var waitBatchCommitTime = aggWaitBatchCommitTime.Count > 0 ? aggWaitBatchCommitTime.Average() : 0;
            var det_networkTime = aggDetNetworkTime.Count > 0 ? aggDetNetworkTime.Average() : 0;
            var det_emitTime = aggDetEmitTime.Count > 0 ? aggDetEmitTime.Average() : 0;
            var det_waitBatchScheduleTime = aggDetWaitBatchScheduleTime.Count > 0 ? aggDetWaitBatchScheduleTime.Average() : 0;
            var det_executeTime = aggDetExecuteTime.Count > 0 ? aggDetExecuteTime.Average() : 0;
            var det_waitBatchCommitTime = aggDetWaitBatchCommitTime.Count > 0 ? aggDetWaitBatchCommitTime.Average() : 0;
            var txnIntraRPCThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(txnIntraRPCThroughputAccumulator.ToArray());
            var txnInterRPCThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(txnInterRPCThroughputAccumulator.ToArray());
            var batchIntraRPCThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(batchIntraRPCThroughputAccumulator.ToArray());
            var batchInterRPCThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(batchInterRPCThroughputAccumulator.ToArray());
            var tokenIntraRPCThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(tokenIntraRPCThroughputAccumulator.ToArray());
            var tokenInterRPCThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(tokenInterRPCThroughputAccumulator.ToArray());
            var detThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(detThroughPutAccumulator.ToArray());
            var nonDetThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(nonDetThroughPutAccumulator.ToArray());
            var abortRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(abortRateAccumulator.ToArray());
            var notSerializableRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(notSerializableRateAccumulator.ToArray());
            var deadlockRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(deadlockRateAccumulator.ToArray());
            using (file = new System.IO.StreamWriter(filePath, true))
            {
                file.Write($"{workload.asyncMsgLengthPerThread} {workload.numAccounts / workload.numAccountsPerGroup} {workload.zipfianConstant} {workload.deterministicTxnPercent}% ");
                if (workload.deterministicTxnPercent > 0) file.Write($"{detThroughputMeanAndSd.Item1} {detThroughputMeanAndSd.Item2} ");
                if (workload.deterministicTxnPercent < 100)
                {
                    file.Write($"{nonDetThroughputMeanAndSd.Item1} {nonDetThroughputMeanAndSd.Item2} ");
                    file.Write($"{abortRateMeanAndSd.Item1}% ");
                    if (workload.deterministicTxnPercent > 0)
                    {
                        var abortRWConflict = 100 - deadlockRateMeanAndSd.Item1 - notSerializableRateMeanAndSd.Item1;
                        file.Write($"{abortRWConflict}% {deadlockRateMeanAndSd.Item1}% {notSerializableRateMeanAndSd.Item1}% ");
                    } 
                } 
                if (Constants.multiSilo)
                {
                    file.Write($"{txnIntraRPCThroughputMeanAndSd.Item1} ");
                    file.Write($"{txnInterRPCThroughputMeanAndSd.Item1} ");
                    file.Write($"{batchIntraRPCThroughputMeanAndSd.Item1} ");
                    file.Write($"{batchInterRPCThroughputMeanAndSd.Item1} ");
                    file.Write($"{tokenIntraRPCThroughputMeanAndSd.Item1} ");
                    file.Write($"{tokenInterRPCThroughputMeanAndSd.Item1} ");
                }
                else
                {
                    file.Write($"{txnIntraRPCThroughputMeanAndSd.Item1} ");
                    file.Write($"{batchIntraRPCThroughputMeanAndSd.Item1} ");
                    file.Write($"{tokenIntraRPCThroughputMeanAndSd.Item1} ");
                }
                if (workload.deterministicTxnPercent > 0)
                {
                    foreach (var percentile in workload.percentilesToCalculate)
                    {
                        var lat = ArrayStatistics.PercentileInplace(aggDetLatencies.ToArray(), percentile);
                        file.Write($" {lat}");
                    }
                    file.Write($" {det_networkTime} {det_emitTime} {det_waitBatchScheduleTime} {det_executeTime} {det_waitBatchCommitTime}");
                }
                if (workload.deterministicTxnPercent < 100)
                {
                    foreach (var percentile in workload.percentilesToCalculate)
                    {
                        var lat = ArrayStatistics.PercentileInplace(aggLatencies.ToArray(), percentile);
                        file.Write($" {lat}");
                    }
                    file.Write($" {networkTime} {emitTime} {executeTime} {waitBatchCommitTime}");
                }
                file.WriteLine();
            }
            /*
            if (workload.deterministicTxnPercent > 0)
            {
                var pact_path = Constants.dataPath + "pact_latency.txt";
                using (var file = new System.IO.StreamWriter(pact_path, true))
                {
                    foreach (var lat in aggDetLatencies) file.Write($"{lat} ");
                    file.WriteLine();
                }
            }
            if (workload.deterministicTxnPercent < 100)
            {
                var act_path = Constants.dataPath + "act_latency.txt";
                using (var file = new System.IO.StreamWriter(act_path, true))
                {
                    foreach (var lat in aggLatencies) file.Write($"{lat} ");
                    file.WriteLine();
                }
            }*/
        }

        private static void WaitForWorkerAcksAndReset()
        {
            ackedWorkers.Wait();
            ackedWorkers.Reset(numWorkerNodes); //Reset for next ack, not thread-safe but provides visibility, ok for us to use due to lock-stepped (distributed producer/consumer) usage pattern i.e., Reset will never called concurrently with other functions (Signal/Wait)            
        }

        static void PushToWorkers()
        {
            // Task Ventilator
            // Binds PUSH socket to tcp://localhost:5557
            // Sends batch of tasks to workers via that socket
            Console.WriteLine("====== PUSH TO WORKERS ======");
            using (var workers = new PublisherSocket(workerAddress))
            {
                Console.WriteLine($"wait for worker to connect");
                //Wait for the workers to connect to controller
                WaitForWorkerAcksAndReset();
                Console.WriteLine($"{numWorkerNodes} worker nodes have connected to Controller");
                //Send the workload configuration
                Console.WriteLine($"Sent workload configuration to {numWorkerNodes} worker nodes");
                var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT);
                msg.contents = serializer.serialize(workload);
                workers.SendMoreFrame("WORKLOAD_INIT").SendFrame(serializer.serialize(msg));
                Console.WriteLine($"Coordinator waits for WORKLOAD_INIT_ACK");
                //Wait for acks for the workload configuration
                WaitForWorkerAcksAndReset();
                Console.WriteLine($"Receive workload configuration ack from {numWorkerNodes} worker nodes");

                for (int i = 0; i < workload.numEpochs; i++)
                {
                    //Send the command to run an epoch
                    Console.WriteLine($"Running Epoch {i} on {numWorkerNodes} worker nodes");

                    if (enableCounter)
                    {
                        if (i > 0)
                        {
                            taskDone = false;
                            GetSetCount(i - 1);
                            while (!taskDone) Thread.Sleep(100);
                        }
                    }

                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH);
                    workers.SendMoreFrame("RUN_EPOCH").SendFrame(serializer.serialize(msg));
                    WaitForWorkerAcksAndReset();
                    Console.WriteLine($"Finished running epoch {i} on {numWorkerNodes} worker nodes");
                }

                if (enableCounter)
                {
                    taskDone = false;
                    GetSetCount(workload.numEpochs - 1);
                    while (!taskDone) Thread.Sleep(100);
                }
            }
        }

        private static async void GetSetCount(int epoch)
        {
            var coordTask = new List<Task<Tuple<int, int, int, int>>>();
            for (int i = 0; i < numCoordinators; i++) coordTask.Add(coords[i].GetSetCount());

            var grainTask = new List<Task<Tuple<int, int>>>();
            for (int i = 0; i < workload.numAccounts / workload.numAccountsPerGroup; i++)
            {
                var grain = client.GetGrain<ICustomerAccountGroupGrain>(i);
                grainTask.Add(grain.GetSetCount());
            }
            await Task.WhenAll(grainTask);
            await Task.WhenAll(coordTask);

            txnIntraCount[epoch] = 0;
            txnInterCount[epoch] = 0;
            batchIntraCount[epoch] = 0;
            batchInterCount[epoch] = 0;
            tokenIntraCount[epoch] = 0;
            tokenInterCount[epoch] = 0;
            for (int i = 0; i < numCoordinators; i++)
            {
                batchIntraCount[epoch] += coordTask[i].Result.Item1;
                batchInterCount[epoch] += coordTask[i].Result.Item2;
                tokenIntraCount[epoch] += coordTask[i].Result.Item3;
                tokenInterCount[epoch] += coordTask[i].Result.Item4;
            }
            for (int i = 0; i < workload.numAccounts / workload.numAccountsPerGroup; i++)
            {
                txnIntraCount[epoch] += grainTask[i].Result.Item1;
                txnInterCount[epoch] += grainTask[i].Result.Item2;
            }
            taskDone = true;
        }

        static void PullFromWorkers()
        {
            // Task Sink
            // Bindd PULL socket to tcp://localhost:5558
            // Collects results from workers via that socket
            Console.WriteLine("====== PULL FROM WORKERS ======");

            results = new WorkloadResults[workload.numEpochs, numWorkerNodes];
            //socket to receive results on
            using (var sink = new PullSocket(sinkAddress))
            {
                for (int i = 0; i < numWorkerNodes; i++)
                {
                    var msg = serializer.deserialize<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                    Trace.Assert(msg.msgType == Utilities.MsgType.WORKER_CONNECT);
                    Console.WriteLine($"Receive WORKER_CONNECT from worker {i}");
                    ackedWorkers.Signal();
                }

                for (int i = 0; i < numWorkerNodes; i++)
                {
                    var msg = serializer.deserialize<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                    Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT_ACK);
                    Console.WriteLine($"Receive WORKLOAD_INIT_ACT from worker {i}");
                    ackedWorkers.Signal();
                }

                //Wait for epoch acks
                for (int i = 0; i < workload.numEpochs; i++)
                {
                    for (int j = 0; j < numWorkerNodes; j++)
                    {
                        var msg = serializer.deserialize<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                        Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH_ACK);
                        results[i, j] = serializer.deserialize<WorkloadResults>(msg.contents);
                        ackedWorkers.Signal();
                    }
                }
            }
        }

        private static async void InitiateClientAndSpawnConfigurationCoordinator()
        {
            //Spawn the configuration grain
            if (client == null)
            {
                ClientConfiguration config = new ClientConfiguration();
                if (Constants.localCluster) client = await config.StartClientWithRetries();
                else client = await config.StartClientWithRetriesToCluster();
            }

            if (workload.grainImplementationType == ImplementationType.SNAPPER)
            {
                var configGrain = client.GetGrain<IConfigurationManagerGrain>(0);
                await configGrain.UpdateNewConfiguration(exeConfig);     // must set exeConfig first!!!!!!
                await configGrain.UpdateNewConfiguration(coordConfig);
                Console.WriteLine("Spawned the configuration grain.");
            }
            asyncInitializationDone = true;
        }

        private static async void LoadGrains()
        {
            Console.WriteLine($"Load grains, numGrains = {workload.numAccounts / workload.numAccountsPerGroup}, numAccountPerGroup = {workload.numAccountsPerGroup}. ");
            var tasks = new List<Task<TransactionResult>>();
            var sequence = false; //If you want to load the grains in sequence instead of all concurrent
            for (int i = 0; i < workload.numAccounts / workload.numAccountsPerGroup; i++)
            {
                var args = new Tuple<int, int>(workload.numAccountsPerGroup, i);
                var input = new FunctionInput(args);

                switch (workload.grainImplementationType)
                {
                    case ImplementationType.ORLEANSEVENTUAL:
                        var etxnGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(i);
                        tasks.Add(etxnGrain.StartTransaction("InitBankAccounts", input));
                        break;
                    case ImplementationType.ORLEANSTXN:
                        var orltxnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(i);
                        tasks.Add(orltxnGrain.StartTransaction("InitBankAccounts", input));
                        break;
                    case ImplementationType.SNAPPER:
                        var sntxnGrain = client.GetGrain<ICustomerAccountGroupGrain>(i);
                        tasks.Add(sntxnGrain.StartTransaction("InitBankAccounts", input));
                        break;
                    default:
                        throw new Exception("Unknown grain implementation type");
                }
                if (sequence)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine("Finish loading grains.");
            loadingDone = true;
        }

        private static void GetWorkloadSettings()
        {
            workload = new WorkloadConfiguration();
            GenerateWorkLoadFromSettingsFile();
        }

        private static async void PrintGrainData()
        {
            var tasks = new List<Task>();

            // print coord data
            for (int i = 0; i < numCoordinators; i++) tasks.Add(coords[i].PrintData());

            // print grain data
            for (int i = 0; i < workload.numAccounts / workload.numAccountsPerGroup; i++)
            {
                var grain = client.GetGrain<ICustomerAccountGroupGrain>(i);
                tasks.Add(grain.PrintData());
            }

            await Task.WhenAll(tasks);
            finishPrint = true;
        }

        static void Main(string[] args)
        {
            if (Constants.multiWorker)
            {
                sinkAddress = Constants.controller_Remote_SinkAddress;
                workerAddress = Constants.controller_Remote_WorkerAddress;
            }
            else
            {
                sinkAddress = Constants.controller_Local_SinkAddress;
                workerAddress = Constants.controller_Local_WorkerAddress;
            }

            serializer = new BinarySerializer();

            //Generate workload configurations interactively            
            GetWorkloadSettings();

            //inject the specially required arguments into workload setting
            workload.zipfianConstant = float.Parse(args[0]);
            workload.deterministicTxnPercent = float.Parse(args[1]);
            vCPU = int.Parse(args[2]);
            /*
            workload.numWorkerNodes = vCPU / 4;
            numWorkerNodes = workload.numWorkerNodes;*/
            ackedWorkers = new CountdownEvent(numWorkerNodes);
            workload.numConnToClusterPerWorkerNode = vCPU;
            workload.numThreadsPerWorkerNode = vCPU;
            workload.numAccounts = 5000 * vCPU;
            coordConfig.numCoordinators = vCPU * 2;
            numCoordinators = coordConfig.numCoordinators;
            Console.WriteLine($"zipf = {workload.zipfianConstant}, detPercent = {workload.deterministicTxnPercent}%, silo_vCPU = {vCPU}, num_thread = {workload.numThreadsPerWorkerNode}, num_coord = {numCoordinators}");

            //Initialize the client to silo cluster, create configurator grain
            InitiateClientAndSpawnConfigurationCoordinator();
            while (!asyncInitializationDone) Thread.Sleep(100);
            Console.WriteLine($"finish initializing coordinators");

            //Create the workload grains, load with data
            LoadGrains();
            while (!loadingDone) Thread.Sleep(100);

            txnIntraCount = new long[workload.numEpochs];
            txnInterCount = new long[workload.numEpochs];
            batchIntraCount = new long[workload.numEpochs];
            batchInterCount = new long[workload.numEpochs];
            tokenIntraCount = new long[workload.numEpochs];
            tokenInterCount = new long[workload.numEpochs];
            coords = new IGlobalTransactionCoordinatorGrain[numCoordinators];
            if (enableCounter)
            {
                for (int i = 0; i < numCoordinators; i++)
                    coords[i] = client.GetGrain<IGlobalTransactionCoordinatorGrain>(i);
            }

            //Start the controller thread
            Thread conducterThread = new Thread(PushToWorkers);
            conducterThread.Start();

            //Start the sink thread
            Thread sinkThread = new Thread(PullFromWorkers);
            sinkThread.Start();

            //Wait for the threads to exit
            sinkThread.Join();
            conducterThread.Join();

            /*
            PrintGrainData();
            while (!finishPrint) Thread.Sleep(100);*/
            if (enableCounter)
            {
                for (int i = 0; i < workload.numEpochs; i++)
                {
                    Console.WriteLine($"epoch {i}: ");
                    Console.WriteLine($"txnIntraCount[{i}] = {txnIntraCount[i]}, txnInterCount[{i}] = {txnInterCount[i]}");
                    Console.WriteLine($"batchIntraCount[{i}] = {batchIntraCount[i]}, batchInterCount[{i}] = {batchInterCount[i]}");
                    Console.WriteLine($"tokenIntraCount[{i}] = {tokenIntraCount[i]}, tokenInterCount[{i}] = {tokenInterCount[i]}");
                }
            }
             
            Console.WriteLine("Aggregating results and printing"); 
            filePath = Constants.dataPath + "result.txt";
            AggregateResultsAndPrint();
            Console.WriteLine("Finished running experiment. Press Enter to exit");
            //Console.ReadLine();
        }
    }
}