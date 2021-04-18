using NetMQ;
using System;
using Orleans;
using System.IO;
using Utilities;
using NewProcess;
using NetMQ.Sockets;
using TPCC.Interfaces;
using System.Threading;
using System.Diagnostics;
using Persist.Interfaces;
using System.Configuration;
using SmallBank.Interfaces;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Statistics;
using System.Collections.Specialized;
using Concurrency.Interface.Nondeterministic;

namespace ExperimentController
{
    class Program
    {
        static int vCPU = 0;
        static string filePath;
        static StreamWriter file;
        static int batchInterval;
        static string sinkAddress;
        static int numWorkerNodes;
        static int numWarmupEpoch;
        static int numCoordinators;
        static string workerAddress;
        static IClusterClient client;
        static ISerializer serializer;
        static WorkloadResults[,] results;
        static CountdownEvent ackedWorkers;
        static ConcurrencyType nonDetCCType;
        static WorkloadConfiguration workload;
        static volatile bool loadingDone = false;
        static LoggingConfiguration loggingConfig;
        static IConfigurationManagerGrain configGrain;
        static volatile bool asyncInitializationDone = false;

        private static void GenerateWorkLoadFromSettingsFile()
        {
            //Parse and initialize benchmarkframework section
            var benchmarkFrameWorkSection = ConfigurationManager.GetSection("BenchmarkFrameworkConfig") as NameValueCollection;
            workload.numWorkerNodes = int.Parse(benchmarkFrameWorkSection["numWorkerNodes"]);
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
            var loggingType = Enum.Parse<LoggingType>(snapperConfigSection["loggingType"]);
            var storageType = Enum.Parse<StorageType>(snapperConfigSection["storageType"]);
            var serializerType = Enum.Parse<SerializerType>(snapperConfigSection["serializerType"]);
            numPersistItem = int.Parse(snapperConfigSection["numPersistItem"]);
            var loggingBatchSize = int.Parse(snapperConfigSection["loggingBatchSize"]);

            //Parse workload specific configuration, assumes only one defined in file
            var benchmarkConfigSection = ConfigurationManager.GetSection("BenchmarkConfig") as NameValueCollection;
            workload.benchmark = Enum.Parse<BenchmarkType>(benchmarkConfigSection["benchmark"]);
            workload.distribution = Enum.Parse<Distribution>(benchmarkConfigSection["distribution"]);
            workload.zipfianConstant = float.Parse(benchmarkConfigSection["zipfianConstant"]);
            workload.deterministicTxnPercent = float.Parse(benchmarkConfigSection["deterministicTxnPercent"]);
            workload.mixture = Array.ConvertAll(benchmarkConfigSection["mixture"].Split(","), x => int.Parse(x));

            workload.numAccounts = int.Parse(benchmarkConfigSection["numAccounts"]);
            workload.numAccountsPerGroup = int.Parse(benchmarkConfigSection["numAccountsPerGroup"]);
            workload.numAccountsMultiTransfer = int.Parse(benchmarkConfigSection["numAccountsMultiTransfer"]);
            workload.numGrainsMultiTransfer = int.Parse(benchmarkConfigSection["numGrainsMultiTransfer"]);
            workload.grainImplementationType = Enum.Parse<ImplementationType>(benchmarkConfigSection["grainImplementationType"]);

            var batching = false;
            if (workload.benchmark == BenchmarkType.TPCC) batching = false;
            loggingConfig = new LoggingConfiguration(loggingType, storageType, serializerType, numPersistItem, loggingBatchSize, batching);
            Console.WriteLine("Generated workload configuration");
        }

        private static void AggregateResultsAndPrint()
        {
            Trace.Assert(workload.numEpochs >= 1);
            Trace.Assert(numWorkerNodes >= 1);
            var aggLatencies = new List<double>();
            var aggDetLatencies = new List<double>();
            var detThroughPutAccumulator = new List<float>();
            var nonDetThroughPutAccumulator = new List<float>();
            var abortRateAccumulator = new List<double>();
            var notSerializableRateAccumulator = new List<float>();
            var notSureSerializableRateAccumulator = new List<float>();
            var deadlockRateAccumulator = new List<float>();
            var ioThroughputAccumulator = new List<float>();
            //Skip the epochs upto warm up epochs
            for (int epochNumber = numWarmupEpoch; epochNumber < workload.numEpochs; epochNumber++)
            {
                int aggNumDetCommitted = results[epochNumber, 0].numDetCommitted;
                int aggNumNonDetCommitted = results[epochNumber, 0].numNonDetCommitted;
                int aggNumDetTransactions = results[epochNumber, 0].numDetTxn;
                int aggNumNonDetTransactions = results[epochNumber, 0].numNonDetTxn;
                int aggNumNotSerializable = results[epochNumber, 0].numNotSerializable;
                int aggNumNotSureSerializable = results[epochNumber, 0].numNotSureSerializable;
                int aggNumDeadlock = results[epochNumber, 0].numDeadlock;
                long aggStartTime = results[epochNumber, 0].startTime;
                long aggEndTime = results[epochNumber, 0].endTime;
                aggLatencies.AddRange(results[epochNumber, 0].latencies);
                aggDetLatencies.AddRange(results[epochNumber, 0].det_latencies);
                for (int workerNode = 1; workerNode < numWorkerNodes; workerNode++)
                {
                    aggNumDetCommitted += results[epochNumber, workerNode].numDetCommitted;
                    aggNumNonDetCommitted += results[epochNumber, workerNode].numNonDetCommitted;
                    aggNumDetTransactions += results[epochNumber, workerNode].numDetTxn;
                    aggNumNonDetTransactions += results[epochNumber, workerNode].numNonDetTxn;
                    aggNumNotSerializable += results[epochNumber, workerNode].numNotSerializable;
                    aggNumNotSureSerializable += results[epochNumber, workerNode].numNotSureSerializable;
                    aggNumDeadlock += results[epochNumber, workerNode].numDeadlock;
                    aggStartTime = (results[epochNumber, workerNode].startTime < aggStartTime) ? results[epochNumber, workerNode].startTime : aggStartTime;
                    aggEndTime = (results[epochNumber, workerNode].endTime < aggEndTime) ? results[epochNumber, workerNode].endTime : aggEndTime;
                    aggLatencies.AddRange(results[epochNumber, workerNode].latencies);
                    aggDetLatencies.AddRange(results[epochNumber, workerNode].det_latencies);
                }
                var time = aggEndTime - aggStartTime;
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
                        var notSureSerializable = aggNumNotSureSerializable * 100.0 / numAbort;   // abort due to incomplete AfterSet
                        notSureSerializableRateAccumulator.Add((float)notSureSerializable);
                        var deadlock = aggNumDeadlock * 100.0 / numAbort;
                        deadlockRateAccumulator.Add((float)deadlock);
                    }
                }
                detThroughPutAccumulator.Add(detCommittedTxnThroughput);
                nonDetThroughPutAccumulator.Add(nonDetCommittedTxnThroughput);
                abortRateAccumulator.Add(abortRate);

                ioThroughputAccumulator.Add((float)IOcount[epochNumber] * 1000 / time);
            }

            //Compute statistics on the accumulators, maybe a better way is to maintain a sorted list
            var detThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(detThroughPutAccumulator.ToArray());
            var nonDetThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(nonDetThroughPutAccumulator.ToArray());
            var abortRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(abortRateAccumulator.ToArray());
            var notSerializableRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(notSerializableRateAccumulator.ToArray());
            var notSureSerializableRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(notSureSerializableRateAccumulator.ToArray());
            var deadlockRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(deadlockRateAccumulator.ToArray());
            var ioThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(ioThroughputAccumulator.ToArray());
            using (file = new System.IO.StreamWriter(filePath, true))
            {
                file.Write($"numWarehouse={workload.numWarehouse} siloCPU={vCPU} distribution={workload.distribution} benchmark={workload.benchmark} ");
                file.Write($"{workload.deterministicTxnPercent}% ");
                if (workload.deterministicTxnPercent > 0) file.Write($"{detThroughputMeanAndSd.Item1} {detThroughputMeanAndSd.Item2} ");
                if (workload.deterministicTxnPercent < 100)
                {
                    file.Write($"{nonDetThroughputMeanAndSd.Item1} {nonDetThroughputMeanAndSd.Item2} ");
                    file.Write($"{abortRateMeanAndSd.Item1}% ");
                    if (workload.deterministicTxnPercent > 0)
                    {
                        var abortRWConflict = 100 - deadlockRateMeanAndSd.Item1 - notSerializableRateMeanAndSd.Item1 - notSureSerializableRateMeanAndSd.Item1;
                        file.Write($"{abortRWConflict}% {deadlockRateMeanAndSd.Item1}% {notSerializableRateMeanAndSd.Item1}% {notSureSerializableRateMeanAndSd.Item1}% ");
                    }
                }
                if (workload.grainImplementationType == ImplementationType.SNAPPER)
                {
                    //file.Write($"{ioThroughputMeanAndSd.Item1} {ioThroughputMeanAndSd.Item2} ");   // number of IOs per second
                }
                if (workload.deterministicTxnPercent > 0)
                {
                    foreach (var percentile in workload.percentilesToCalculate)
                    {
                        var lat = ArrayStatistics.PercentileInplace(aggDetLatencies.ToArray(), percentile);
                        file.Write($"{lat} ");
                    }
                }
                if (workload.deterministicTxnPercent < 100)
                {
                    foreach (var percentile in workload.percentilesToCalculate)
                    {
                        var lat = ArrayStatistics.PercentileInplace(aggLatencies.ToArray(), percentile);
                        file.Write($"{lat} ");
                    }
                }
                file.WriteLine();
            }
            /*
            if (workload.deterministicTxnPercent == 100) filePath = Constants.dataPath + $"PACT_{workload.numAccountsMultiTransfer}.txt";
            if (workload.deterministicTxnPercent == 0)
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

                IOcount = new long[workload.numEpochs];
                for (int i = 0; i < workload.numEpochs; i++)
                {
                    if (workload.grainImplementationType == ImplementationType.SNAPPER)
                    {
                        SetIOCount();
                        while (!setCountFinish) Thread.Sleep(100);
                        setCountFinish = false;
                    }

                    //Send the command to run an epoch
                    Console.WriteLine($"Running Epoch {i} on {numWorkerNodes} worker nodes");
                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH);
                    workers.SendMoreFrame("RUN_EPOCH").SendFrame(serializer.serialize(msg));
                    WaitForWorkerAcksAndReset();
                    Console.WriteLine($"Finished running epoch {i} on {numWorkerNodes} worker nodes");

                    if (workload.grainImplementationType == ImplementationType.SNAPPER)
                    {
                        GetIOCount(i);
                        while (!getCountFinish) Thread.Sleep(100);
                        getCountFinish = false;
                    }
                    if (workload.benchmark == BenchmarkType.TPCC)
                    {
                        ResetOrderGrain();
                        while (!resetFinish) Thread.Sleep(100);
                        resetFinish = false;
                    }
                }
            }
        }

        static bool setCountFinish = false;
        static bool getCountFinish = false;
        static int numPersistItem = 0;
        static long[] IOcount;

        static async void SetIOCount()
        {
            switch (workload.grainImplementationType)
            {
                case ImplementationType.SNAPPER:
                    if (loggingConfig.loggingType == LoggingType.PERSISTSINGLETON) await configGrain.SetIOCount();
                    if (loggingConfig.loggingType == LoggingType.PERSISTGRAIN)
                    {
                        var tasks = new List<Task>();
                        for (int i = 0; i < numPersistItem; i++)
                        {
                            var grain = client.GetGrain<IPersistGrain>(i);
                            tasks.Add(grain.SetIOCount());
                        }
                        await Task.WhenAll(tasks);
                    }
                    break;
                case ImplementationType.ORLEANSTXN:
                    var txngrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(0);
                    if (loggingConfig.loggingType == LoggingType.PERSISTSINGLETON) await txngrain.SetIOCount();
                    break;
            }
            setCountFinish = true;
        }

        static async void GetIOCount(int epoch)
        {
            IOcount[epoch] = 0;
            switch (workload.grainImplementationType)
            {
                case ImplementationType.SNAPPER:
                    if (loggingConfig.loggingType == LoggingType.PERSISTSINGLETON) IOcount[epoch] = await configGrain.GetIOCount();
                    if (loggingConfig.loggingType == LoggingType.PERSISTGRAIN)
                    {
                        var tasks = new List<Task<long>>();
                        for (int i = 0; i < numPersistItem; i++)
                        {
                            var grain = client.GetGrain<IPersistGrain>(i);
                            tasks.Add(grain.GetIOCount());
                        }
                        await Task.WhenAll(tasks);
                        foreach (var t in tasks) IOcount[epoch] += t.Result;
                    }
                    break;
                case ImplementationType.ORLEANSEVENTUAL:
                    var txngrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(0);
                    Debug.Assert(loggingConfig.loggingType == LoggingType.PERSISTSINGLETON);
                    await txngrain.GetIOCount();
                    break;
            }
            getCountFinish = true;
        }

        static bool resetFinish = false;
        static async void ResetOrderGrain()
        {
            // set OrderGrain as empty table
            var index = 0;
            var tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < workload.numWarehouse; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    for (int i = 0; i < Constants.NUM_OrderGrain_PER_D; i++)
                    {
                        index = W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * Constants.NUM_OrderGrain_PER_D + i;
                        var input = new Tuple<int, int, int>(W_ID, D_ID, i);
                        if (workload.grainImplementationType == ImplementationType.ORLEANSEVENTUAL)
                        {
                            var grain = client.GetGrain<IEventualOrderGrain>(index);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        else
                        {
                            var grain = client.GetGrain<IOrderGrain>(index);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        if (tasks.Count == Environment.ProcessorCount)
                        {
                            await Task.WhenAll(tasks);
                            tasks.Clear();
                        }
                    }
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish re-setting {workload.numOrderGrain} OrderGrain. ");
            resetFinish = true;
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
                configGrain = client.GetGrain<IConfigurationManagerGrain>(0);
                await configGrain.UpdateConfiguration(loggingConfig);
                await configGrain.UpdateConfiguration(nonDetCCType);
                await configGrain.UpdateConfiguration(numCoordinators);
                Console.WriteLine("Spawned the configuration grain.");
            }
            asyncInitializationDone = true;
        }

        private static async void LoadSmallBankGrains()
        {
            int numGrain = workload.numAccounts / workload.numAccountsPerGroup;
            Console.WriteLine($"Load grains, benchmark {workload.benchmark}, numGrains = {numGrain}");
            var tasks = new List<Task<TransactionResult>>();
            var sequence = false;   // load the grains in sequence instead of all concurrent
            if (loggingConfig.loggingType != LoggingType.NOLOGGING) sequence = true;
            var start = DateTime.Now;
            for (int i = 0; i < numGrain; i++)
            {
                var input = new Tuple<int, int>(workload.numAccountsPerGroup, i);
                switch (workload.grainImplementationType)
                {
                    case ImplementationType.ORLEANSEVENTUAL:
                        var etxnGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(i);
                        tasks.Add(etxnGrain.StartTransaction("Init", input));
                        break;
                    case ImplementationType.ORLEANSTXN:
                        var orltxnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(i);
                        tasks.Add(orltxnGrain.StartTransaction("Init", input));
                        break;
                    case ImplementationType.SNAPPER:
                        var sntxnGrain = client.GetGrain<ICustomerAccountGroupGrain>(i);
                        tasks.Add(sntxnGrain.StartTransaction("Init", input));
                        break;
                    default:
                        throw new Exception("Unknown grain implementation type");
                }

                if (sequence && tasks.Count == 200)
                {
                    Console.WriteLine($"Load {Environment.ProcessorCount} grains, i = {i}");
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading grains, it takes {(DateTime.Now - start).TotalSeconds}s.");
            loadingDone = true;
        }

        private static async void LoadTPCCGrains()
        {
            Debug.Assert(workload.grainImplementationType != ImplementationType.ORLEANSTXN);
            var eventual = workload.grainImplementationType == ImplementationType.ORLEANSEVENTUAL;
            Console.WriteLine($"Load grains, benchmark {workload.benchmark}, {workload.grainImplementationType}. ");
            var sequence = true;   // load the grains in sequence instead of all concurrent
            var start = DateTime.Now;

            // load ItemGrain
            for (int W_ID = 0; W_ID < workload.numWarehouse; W_ID++)
            {
                var grainID = Helper.GetItemGrain(W_ID);
                if (eventual)
                {
                    var grain = client.GetGrain<IEventualItemGrain>(grainID);
                    await grain.StartTransaction("Init", null);
                }
                else
                {
                    var grain = client.GetGrain<IItemGrain>(grainID);
                    await grain.StartTransaction("Init", null);
                }
            }
            Console.WriteLine($"Finish loading {workload.numItemGrain} ItemGrain. ");

            // load WarehouseGrain
            for (int W_ID = 0; W_ID < workload.numWarehouse; W_ID++)
            {
                var grainID = Helper.GetWarehouseGrain(W_ID);
                if (eventual)
                {
                    var grain = client.GetGrain<IEventualWarehouseGrain>(grainID);
                    await grain.StartTransaction("Init", W_ID);
                }
                else
                {
                    var grain = client.GetGrain<IWarehouseGrain>(grainID);
                    await grain.StartTransaction("Init", W_ID);
                }
            }
            Console.WriteLine($"Finish loading {workload.numWarehouseGrain} WarehouseGrain. ");

            // load DistrictGrain and CustomerGrain
            var districtGrainID = 0;
            var customerGrainID = 0;
            var tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < workload.numWarehouse; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    districtGrainID = Helper.GetDistrictGrain(W_ID, D_ID);
                    customerGrainID = Helper.GetCustomerGrain(W_ID, D_ID);
                    var input = new Tuple<int, int>(W_ID, D_ID);
                    if (eventual)
                    {
                        var districtGrain = client.GetGrain<IEventualDistrictGrain>(districtGrainID);
                        tasks.Add(districtGrain.StartTransaction("Init", input));
                        var customerGrain = client.GetGrain<IEventualCustomerGrain>(customerGrainID);
                        tasks.Add(customerGrain.StartTransaction("Init", input));
                    }
                    else
                    {
                        var districtGrain = client.GetGrain<IDistrictGrain>(districtGrainID);
                        tasks.Add(districtGrain.StartTransaction("Init", input));
                        var customerGrain = client.GetGrain<ICustomerGrain>(customerGrainID);
                        tasks.Add(customerGrain.StartTransaction("Init", input));
                    }
                    if (sequence && tasks.Count == Environment.ProcessorCount)
                    {
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {workload.numDistrictGrain} DistrictGrain and {workload.numCustomerGrain} CustomerGrain. ");

            // load StockGrain
            var stockGrainID = 0;
            tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < workload.numWarehouse; W_ID++)
            {
                for (int i = 0; i < Constants.NUM_StockGrain_PER_W; i++)
                {
                    stockGrainID = W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + i;
                    var input = new Tuple<int, int>(W_ID, i);
                    if (eventual)
                    {
                        var grain = client.GetGrain<IEventualStockGrain>(stockGrainID);
                        tasks.Add(grain.StartTransaction("Init", input));
                    }
                    else
                    {
                        var grain = client.GetGrain<IStockGrain>(stockGrainID);
                        tasks.Add(grain.StartTransaction("Init", input));
                    }
                    if (sequence && tasks.Count == Environment.ProcessorCount)
                    {
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {workload.numStockGrain} StockGrain. ");

            // load OrderGrain
            var orderGrainID = 0;
            tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < workload.numWarehouse; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    for (int i = 0; i < Constants.NUM_OrderGrain_PER_D; i++)
                    {
                        orderGrainID = W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * Constants.NUM_OrderGrain_PER_D + i;
                        var input = new Tuple<int, int, int>(W_ID, D_ID, i);
                        if (eventual)
                        {
                            var grain = client.GetGrain<IEventualOrderGrain>(orderGrainID);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        else
                        {
                            var grain = client.GetGrain<IOrderGrain>(orderGrainID);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        if (sequence && tasks.Count == Environment.ProcessorCount)
                        {
                            await Task.WhenAll(tasks);
                            tasks.Clear();
                        }
                    }
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {workload.numOrderGrain} OrderGrain. ");

            Console.WriteLine($"Finish loading grains, it takes {(DateTime.Now - start).TotalSeconds}s.");
            loadingDone = true;
        }

        private static void GetWorkloadSettings()
        {
            workload = new WorkloadConfiguration();
            GenerateWorkLoadFromSettingsFile();
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

            if (workload.grainImplementationType == ImplementationType.SNAPPER)
            {
                numCoordinators = vCPU * 2;
                loggingConfig.numPersistItem = vCPU * 2;
                numPersistItem = loggingConfig.numPersistItem;
            }
            if (workload.benchmark == BenchmarkType.SMALLBANK) workload.numAccounts = 5000 * vCPU;
            if (workload.benchmark == BenchmarkType.TPCC)
            {
                workload.numWarehouse = vCPU * Constants.NUM_W_PER_4CORE / 4;
                workload.numItemGrain = workload.numWarehouse;
                workload.numWarehouseGrain = workload.numWarehouse;
                workload.numCustomerGrain = workload.numWarehouse * Constants.NUM_D_PER_W;
                workload.numDistrictGrain = workload.numWarehouse * Constants.NUM_D_PER_W;
                workload.numStockGrain = workload.numWarehouse * Constants.NUM_StockGrain_PER_W;
                workload.numOrderGrain = workload.numWarehouse * Constants.NUM_D_PER_W * Constants.NUM_OrderGrain_PER_D;
            }
            Console.WriteLine($"worker node = {workload.numWorkerNodes}, silo_vCPU = {vCPU}, detPercent = {workload.deterministicTxnPercent}%, num_coord = {numCoordinators}, num_warehouse = {workload.numWarehouse}, numPersistItem = {numPersistItem}");
            numWorkerNodes = workload.numWorkerNodes;
            ackedWorkers = new CountdownEvent(numWorkerNodes);

            //Initialize the client to silo cluster, create configurator grain
            InitiateClientAndSpawnConfigurationCoordinator();
            while (!asyncInitializationDone) Thread.Sleep(100);
            Console.WriteLine($"finish initializing coordinators");

            //Create the workload grains, load with data
            if (workload.benchmark == BenchmarkType.SMALLBANK) LoadSmallBankGrains();
            if (workload.benchmark == BenchmarkType.TPCC) LoadTPCCGrains();
            while (!loadingDone) Thread.Sleep(100);

            //Start the controller thread
            Thread conducterThread = new Thread(PushToWorkers);
            conducterThread.Start();

            //Start the sink thread
            Thread sinkThread = new Thread(PullFromWorkers);
            sinkThread.Start();

            //Wait for the threads to exit
            sinkThread.Join();
            conducterThread.Join();

            Console.WriteLine("Aggregating results and printing");
            filePath = Constants.dataPath + "result.txt";
            AggregateResultsAndPrint();
            Console.WriteLine("Finished running experiment. Press Enter to exit");
            //Console.ReadLine();
        }
    }
}