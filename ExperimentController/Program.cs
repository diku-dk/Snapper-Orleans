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

namespace ExperimentController
{
    class Program
    {
        static string filePath;
        static StreamWriter file;
        static string sinkAddress;
        static int numWarmupEpoch;
        static string workerAddress;
        static IClusterClient client;
        static ISerializer serializer;
        static WorkloadResults[,] results;
        static CountdownEvent ackedWorkers;
        static WorkloadConfiguration workload;
        static volatile bool loadingDone = false;
        static IConfigurationManagerGrain configGrain;
        static volatile bool asyncInitializationDone = false;

        private static void GenerateWorkLoadFromSettingsFile()
        {
            // Parse and initialize benchmarkframework section
            var benchmarkFrameWorkSection = ConfigurationManager.GetSection("BenchmarkFrameworkConfig") as NameValueCollection;
            workload.numEpochs = int.Parse(benchmarkFrameWorkSection["numEpoch"]);
            numWarmupEpoch = int.Parse(benchmarkFrameWorkSection["numWarmupEpoch"]);
            workload.epochDurationMSecs = int.Parse(benchmarkFrameWorkSection["epochDurationMSecs"]);
            workload.numThreadsPerWorkerNode = int.Parse(benchmarkFrameWorkSection["numThreadsPerWorkerNode"]);
            workload.numConnToClusterPerWorkerNode = int.Parse(benchmarkFrameWorkSection["numConnToClusterPerWorkerNode"]);
            workload.percentilesToCalculate = Array.ConvertAll(benchmarkFrameWorkSection["percentilesToCalculate"].Split(","), x => int.Parse(x));

            // Parse workload specific configuration, assumes only one defined in file
            var benchmarkConfigSection = ConfigurationManager.GetSection("BenchmarkConfig") as NameValueCollection;
            workload.benchmark = Enum.Parse<BenchmarkType>(benchmarkConfigSection["benchmark"]);
            workload.txnSize = int.Parse(benchmarkConfigSection["txnSize"]);
            workload.actPipeSize = int.Parse(benchmarkConfigSection["actPipeSize"]);
            workload.pactPipeSize = int.Parse(benchmarkConfigSection["pactPipeSize"]);
            workload.distribution = Enum.Parse<Distribution>(benchmarkConfigSection["distribution"]);
            workload.txnSkewness = float.Parse(benchmarkConfigSection["txnSkewness"]);
            workload.grainSkewness = float.Parse(benchmarkConfigSection["grainSkewness"]);
            workload.zipfianConstant = float.Parse(benchmarkConfigSection["zipfianConstant"]);
            workload.pactPercent = int.Parse(benchmarkConfigSection["pactPercent"]);
            Console.WriteLine("Generated workload configuration");
        }

        private static void AggregateResultsAndPrint()
        {
            Trace.Assert(workload.numEpochs >= 1);
            Trace.Assert(Constants.numWorker >= 1);
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

                for (int workerNode = 1; workerNode < Constants.numWorker; workerNode++)
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
                if (workload.pactPercent < 100)
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
            using (file = new StreamWriter(filePath, true))
            {
                //file.Write($"numWarehouse={Constants.NUM_W_PER_SILO} siloCPU={Constants.numCPUPerSilo} distribution={workload.distribution} benchmark={workload.benchmark} ");
                //file.Write($"{workload.pactPercent}% ");
                if (workload.pactPercent > 0) file.Write($"{detThroughputMeanAndSd.Item1:0} {detThroughputMeanAndSd.Item2:0} ");
                if (workload.pactPercent < 100)
                {
                    file.Write($"{nonDetThroughputMeanAndSd.Item1:0} {nonDetThroughputMeanAndSd.Item2:0} ");
                    //file.Write($"{abortRateMeanAndSd.Item1}% ");
                    if (workload.pactPercent > 0)
                    {
                        var abortRWConflict = 100 - deadlockRateMeanAndSd.Item1 - notSerializableRateMeanAndSd.Item1 - notSureSerializableRateMeanAndSd.Item1;
                        file.Write($"{Math.Round(abortRWConflict, 2).ToString().Replace(',', '.')}% {Math.Round(deadlockRateMeanAndSd.Item1, 2).ToString().Replace(',', '.')}% {Math.Round(notSerializableRateMeanAndSd.Item1, 2).ToString().Replace(',', '.')}% {Math.Round(notSureSerializableRateMeanAndSd.Item1, 2).ToString().Replace(',', '.')}% ");
                    }
                }
                if (Constants.implementationType == ImplementationType.SNAPPER)
                {
                    //file.Write($"{ioThroughputMeanAndSd.Item1} {ioThroughputMeanAndSd.Item2} ");   // number of IOs per second
                }
                if (workload.pactPercent > 0)
                {
                    foreach (var percentile in workload.percentilesToCalculate)
                    {
                        var lat = ArrayStatistics.PercentileInplace(aggDetLatencies.ToArray(), percentile);
                        file.Write($"{Math.Round(lat, 2).ToString().Replace(',', '.')} ");
                    }
                }
                if (workload.pactPercent < 100)
                {
                    foreach (var percentile in workload.percentilesToCalculate)
                    {
                        var lat = ArrayStatistics.PercentileInplace(aggLatencies.ToArray(), percentile);
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

        private static void WaitForWorkerAcksAndReset()
        {
            ackedWorkers.Wait();
            ackedWorkers.Reset(Constants.numWorker); //Reset for next ack, not thread-safe but provides visibility, ok for us to use due to lock-stepped (distributed producer/consumer) usage pattern i.e., Reset will never called concurrently with other functions (Signal/Wait)            
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
                Console.WriteLine($"{Constants.numWorker} worker nodes have connected to Controller");
                //Send the workload configuration
                Console.WriteLine($"Sent workload configuration to {Constants.numWorker} worker nodes");
                var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT);
                msg.contents = serializer.serialize(workload);
                workers.SendMoreFrame("WORKLOAD_INIT").SendFrame(serializer.serialize(msg));
                Console.WriteLine($"Coordinator waits for WORKLOAD_INIT_ACK");
                //Wait for acks for the workload configuration
                WaitForWorkerAcksAndReset();
                Console.WriteLine($"Receive workload configuration ack from {Constants.numWorker} worker nodes");

                IOcount = new long[workload.numEpochs];
                for (int i = 0; i < workload.numEpochs; i++)
                {
                    if (Constants.implementationType == ImplementationType.SNAPPER)
                    {
                        SetIOCount();
                        while (!setCountFinish) Thread.Sleep(100);
                        setCountFinish = false;
                    }

                    //Send the command to run an epoch
                    Console.WriteLine($"Running Epoch {i} on {Constants.numWorker} worker nodes");
                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH);
                    workers.SendMoreFrame("RUN_EPOCH").SendFrame(serializer.serialize(msg));
                    WaitForWorkerAcksAndReset();
                    Console.WriteLine($"Finished running epoch {i} on {Constants.numWorker} worker nodes");

                    if (Constants.implementationType == ImplementationType.SNAPPER)
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
        static long[] IOcount;

        static async void SetIOCount()
        {
            switch (Constants.implementationType)
            {
                case ImplementationType.SNAPPER:
                    if (Constants.loggingType == LoggingType.PERSISTSINGLETON) await configGrain.SetIOCount();
                    if (Constants.loggingType == LoggingType.PERSISTGRAIN)
                    {
                        var tasks = new List<Task>();
                        for (int i = 0; i < Constants.numPersistItemPerSilo; i++)
                        {
                            var grain = client.GetGrain<IPersistGrain>(i);
                            tasks.Add(grain.SetIOCount());
                        }
                        await Task.WhenAll(tasks);
                    }
                    break;
                case ImplementationType.ORLEANSTXN:
                    var txngrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(0);
                    if (Constants.loggingType == LoggingType.PERSISTSINGLETON) await txngrain.SetIOCount();
                    break;
            }
            setCountFinish = true;
        }

        static async void GetIOCount(int epoch)
        {
            IOcount[epoch] = 0;
            switch (Constants.implementationType)
            {
                case ImplementationType.SNAPPER:
                    if (Constants.loggingType == LoggingType.PERSISTSINGLETON) IOcount[epoch] = await configGrain.GetIOCount();
                    if (Constants.loggingType == LoggingType.PERSISTGRAIN)
                    {
                        var tasks = new List<Task<long>>();
                        for (int i = 0; i < Constants.numPersistItemPerSilo; i++)
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
                    Debug.Assert(Constants.loggingType == LoggingType.PERSISTSINGLETON);
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
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    for (int i = 0; i < Constants.NUM_OrderGrain_PER_D; i++)
                    {
                        index = W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * Constants.NUM_OrderGrain_PER_D + i;
                        var input = new Tuple<int, int, int>(W_ID, D_ID, i);
                        if (Constants.implementationType == ImplementationType.ORLEANSEVENTUAL)
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
            Console.WriteLine($"Finish re-setting OrderGrain. ");
            resetFinish = true;
        }

        static void PullFromWorkers()
        {
            // Task Sink
            // Bindd PULL socket to tcp://localhost:5558
            // Collects results from workers via that socket
            Console.WriteLine("====== PULL FROM WORKERS ======");

            results = new WorkloadResults[workload.numEpochs, Constants.numWorker];
            //socket to receive results on
            using (var sink = new PullSocket(sinkAddress))
            {
                for (int i = 0; i < Constants.numWorker; i++)
                {
                    var msg = serializer.deserialize<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                    Trace.Assert(msg.msgType == Utilities.MsgType.WORKER_CONNECT);
                    Console.WriteLine($"Receive WORKER_CONNECT from worker {i}");
                    ackedWorkers.Signal();
                }

                for (int i = 0; i < Constants.numWorker; i++)
                {
                    var msg = serializer.deserialize<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                    Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT_ACK);
                    Console.WriteLine($"Receive WORKLOAD_INIT_ACT from worker {i}");
                    ackedWorkers.Signal();
                }

                //Wait for epoch acks
                for (int i = 0; i < workload.numEpochs; i++)
                {
                    for (int j = 0; j < Constants.numWorker; j++)
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

            if (Constants.implementationType == ImplementationType.SNAPPER)
            {
                configGrain = client.GetGrain<IConfigurationManagerGrain>(0);
                await configGrain.Initialize();
                Console.WriteLine("Spawned the configuration grain.");
            }
            asyncInitializationDone = true;
        }

        private static async void LoadSmallBankGrains()
        {
            int numGrain = Constants.numGrainPerSilo;
            Console.WriteLine($"Load grains, benchmark {workload.benchmark}, numGrains = {numGrain}");
            var tasks = new List<Task<TransactionResult>>();
            var sequence = false;   // load the grains in sequence instead of all concurrent
            if (Constants.loggingType != LoggingType.NOLOGGING) sequence = true;
            var start = DateTime.Now;
            for (int i = 0; i < numGrain; i++)
            {
                var input = new Tuple<int, int>(1, i);
                switch (Constants.implementationType)
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
            Debug.Assert(Constants.implementationType != ImplementationType.ORLEANSTXN);
            var eventual = Constants.implementationType == ImplementationType.ORLEANSEVENTUAL;
            Console.WriteLine($"Load grains, benchmark {workload.benchmark}, {Constants.implementationType}. ");
            var sequence = true;   // load the grains in sequence instead of all concurrent
            var start = DateTime.Now;

            // load ItemGrain
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
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
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO} ItemGrain. ");

            // load WarehouseGrain
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
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
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO} WarehouseGrain. ");

            // load DistrictGrain and CustomerGrain
            int districtGrainID;
            int customerGrainID;
            var tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
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
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO * Constants.NUM_D_PER_W} DistrictGrain and {Constants.NUM_W_PER_SILO * Constants.NUM_D_PER_W} CustomerGrain. ");

            // load StockGrain
            int stockGrainID;
            tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
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
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO * Constants.NUM_StockGrain_PER_W} StockGrain. ");

            // load OrderGrain
            int orderGrainID;
            tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
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
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO * Constants.NUM_D_PER_W * Constants.NUM_OrderGrain_PER_D} OrderGrain. ");

            Console.WriteLine($"Finish loading grains, it takes {(DateTime.Now - start).TotalSeconds}s.");
            loadingDone = true;
        }

        private static void GetWorkloadSettings()
        {
            workload = new WorkloadConfiguration();
            GenerateWorkLoadFromSettingsFile();
        }

        static void Main()
        {
            if (Constants.numWorker > 1)
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

            // Generate workload configurations interactively            
            GetWorkloadSettings();

            Console.WriteLine($"worker node = {Constants.numWorker}, silo_vCPU = {Constants.numCPUPerSilo}, detPercent = {workload.pactPercent}%");
            ackedWorkers = new CountdownEvent(Constants.numWorker);

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