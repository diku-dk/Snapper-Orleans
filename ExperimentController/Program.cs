using System;
using System.Configuration;
using System.Collections.Specialized;
using NetMQ.Sockets;
using NetMQ;
using System.Diagnostics;
using Utilities;
using System.Threading;
using Orleans;
using ExperimentProcess;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using System.Collections.Generic;
using System.Threading.Tasks;
using SmallBank.Interfaces;
using MathNet.Numerics.Statistics;

namespace ExperimentController
{
    class Program
    {
        static String workerAddress = "@tcp://localhost:5575";
        static String sinkAddress = ">tcp://localhost:5558";
        static int numWorkerNodes;
        static int numWarmupEpoch;
        static IClusterClient client;
        static Boolean LocalCluster;
        static IConfigurationManagerGrain configGrain;
        static bool asyncInitializationDone = false;
        static bool loadingDone = false;
        static CountdownEvent ackedWorkers;
        static WorkloadConfiguration workload;
        static ExecutionGrainConfiguration exeConfig;
        static CoordinatorGrainConfiguration coordConfig;
        static WorkloadResults[,] results;

        private static void GenerateWorkLoadFromSettingsFile()
        {
            //Parse and initialize benchmarkframework section
            var benchmarkFrameWorkSection = ConfigurationManager.GetSection("BenchmarkFrameworkConfig") as NameValueCollection;
            LocalCluster = bool.Parse(benchmarkFrameWorkSection["LocalCluster"]);
            workload.numWorkerNodes = int.Parse(benchmarkFrameWorkSection["numWorkerNodes"]);
            numWorkerNodes = workload.numWorkerNodes;
            workload.numConnToClusterPerWorkerNode = int.Parse(benchmarkFrameWorkSection["numConnToClusterPerWorkerNode"]);
            workload.numThreadsPerWorkerNode = int.Parse(benchmarkFrameWorkSection["numThreadsPerWorkerNode"]);
            workload.epochDurationMSecs = int.Parse(benchmarkFrameWorkSection["epochDurationMSecs"]);
            workload.numEpochs = int.Parse(benchmarkFrameWorkSection["numEpochs"]);
            numWarmupEpoch = int.Parse(benchmarkFrameWorkSection["numWarmupEpoch"]);
            workload.asyncMsgLengthPerThread = int.Parse(benchmarkFrameWorkSection["asyncMsgLengthPerThread"]);
            workload.percentilesToCalculate = Array.ConvertAll<string,int>(benchmarkFrameWorkSection["percentilesToCalculate"].Split(","), x=>int.Parse(x));

            //Parse Snapper configuration
            var snapperConfigSection = ConfigurationManager.GetSection("SnapperConfig") as NameValueCollection;
            var nonDetCCType = Enum.Parse<ConcurrencyType>(snapperConfigSection["nonDetCCType"]);
            var maxNonDetWaitingLatencyInMSecs = int.Parse(snapperConfigSection["maxNonDetWaitingLatencyInMSecs"]);
            var batchIntervalMSecs = int.Parse(snapperConfigSection["batchIntervalMSecs"]);
            var idleIntervalTillBackOffSecs = int.Parse(snapperConfigSection["idleIntervalTillBackOffSecs"]);
            var backoffIntervalMsecs = int.Parse(snapperConfigSection["backoffIntervalMsecs"]);            
            var numCoordinators = uint.Parse(snapperConfigSection["numCoordinators"]);
            //Create the configuration objects to be used for ConfigurationGrain
            exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), maxNonDetWaitingLatencyInMSecs);
            coordConfig = new CoordinatorGrainConfiguration(batchIntervalMSecs, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numCoordinators);

            //Parse workload specific configuration, assumes only one defined in file
            var benchmarkConfigSection = ConfigurationManager.GetSection("BenchmarkConfig") as NameValueCollection;
            workload.benchmark = Enum.Parse<BenchmarkType>(benchmarkConfigSection["benchmark"]);
            workload.distribution = Enum.Parse<Distribution>(benchmarkConfigSection["distribution"]);
            workload.zipfianConstant = float.Parse(benchmarkConfigSection["zipfianConstant"]);
            workload.deterministicTxnPercent = float.Parse(benchmarkConfigSection["deterministicTxnPercent"]);            
            workload.mixture = Array.ConvertAll<string, int>(benchmarkConfigSection["mixture"].Split(","), x => int.Parse(x));
            switch (workload.benchmark)
            {
                case BenchmarkType.SMALLBANK:
                    workload.numAccounts = uint.Parse(benchmarkConfigSection["numAccounts"]);
                    workload.numAccountsPerGroup = uint.Parse(benchmarkConfigSection["numAccountsPerGroup"]);
                    workload.numAccountsMultiTransfer = int.Parse(benchmarkConfigSection["numAccountsMultiTransfer"]);
                    workload.numGrainsMultiTransfer = int.Parse(benchmarkConfigSection["numGrainsMultiTransfer"]);
                    workload.grainImplementationType = Enum.Parse<ImplementationType>(benchmarkConfigSection["grainImplementationType"]);
                    break;
                default:
                    throw new Exception("Unknown benchmark type");
            }
            Console.WriteLine("Generated workload configuration");

        }
        private static void AggregateResultsAndPrint() {
            Trace.Assert(workload.numEpochs >= 1);
            Trace.Assert(numWorkerNodes >= 1);
            var aggLatencies = new List<double>();
            var throughPutAccumulator = new List<float>();
            var abortRateAccumulator = new List<float>();
            //Skip the epochs upto warm up epochs
            for (int epochNumber = numWarmupEpoch; epochNumber < workload.numEpochs; epochNumber++)
            {                
                int aggNumCommitted = results[epochNumber,0].numCommitted;
                int aggNumTransactions = results[epochNumber, 0].numTransactions;
                long aggStartTime = results[epochNumber,0].startTime;
                long aggEndTime = results[epochNumber,0].endTime;
                aggLatencies.AddRange(results[epochNumber, 0].latencies);
                for (int workerNode = 1; workerNode < numWorkerNodes; workerNode++)
                {
                    aggNumCommitted += results[epochNumber,workerNode].numCommitted;
                    aggNumTransactions += results[epochNumber,workerNode].numTransactions;
                    aggStartTime = (results[epochNumber,workerNode].startTime < aggStartTime) ? results[epochNumber,workerNode].startTime : aggStartTime;
                    aggEndTime = (results[epochNumber,workerNode].endTime < aggEndTime) ? results[epochNumber,workerNode].endTime : aggEndTime;
                    aggLatencies.AddRange(results[epochNumber,workerNode].latencies);
                }
                float committedTxnThroughput = (float)aggNumCommitted * 1000 / (float) (aggEndTime - aggStartTime);
                float abortRate = (float)(aggNumTransactions - aggNumCommitted) / (float) aggNumTransactions;
                throughPutAccumulator.Add(committedTxnThroughput);
                abortRateAccumulator.Add(abortRate);
            }
            //Compute statistics on the accumulators, maybe a better way is to maintain a sorted list
            var throughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(throughPutAccumulator.ToArray());
            var abortRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(abortRateAccumulator.ToArray());
            Console.WriteLine($"Results across {workload.numEpochs} with first {numWarmupEpoch} epochs being for warmup follows");
            Console.WriteLine($"Mean Throughput per second = { throughputMeanAndSd.Item1}, standard deviation = { throughputMeanAndSd.Item2}");
            Console.WriteLine($"Mean Abort rate (%) = { abortRateMeanAndSd.Item1}, standard deviation = { abortRateMeanAndSd.Item2}");
            //Compute quantiles
            //var aggLatenciesArray = Array.ConvertAll(aggLatencies.ToArray(), e => Convert.ToDouble(e));
            //var aggLatenciesArray = aggLatencies.ToArray();
            Console.Write("Latency (msec) Percentiles follow ");
            foreach (var percentile in workload.percentilesToCalculate)
            {
                var lat = ArrayStatistics.PercentileInplace(aggLatencies.ToArray(), percentile);
                Console.Write($", {percentile} = {lat}");
            }
            Console.WriteLine();
        }
        private static void WaitForWorkerAcksAndReset() {
            ackedWorkers.Wait();
            ackedWorkers.Reset(numWorkerNodes); //Reset for next ack, not thread-safe but provides visibility, ok for us to use due to lock-stepped (distributed producer/consumer) usage pattern i.e., Reset will never called concurrently with other functions (Signal/Wait)            
        }
        static void PushToWorkers() {
            // Task Ventilator
            // Binds PUSH socket to tcp://localhost:5557
            // Sends batch of tasks to workers via that socket
            Console.WriteLine("====== VENTILATOR ======");
            using (var workers = new PushSocket(workerAddress))
            {                
                //Wait for the workers to connect to controller
                WaitForWorkerAcksAndReset();
                //Send the workload configuration
                Console.WriteLine($"{numWorkerNodes} worker nodes have connected to Controller");
                var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT);
                msg.contents = Helper.serializeToByteArray<WorkloadConfiguration>(workload);
                workers.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                Console.WriteLine($"Sent workload configuration to {numWorkerNodes} worker nodes");
                //Wait for acks for the workload configuration
                WaitForWorkerAcksAndReset();

                for(int i=0;i<workload.numEpochs;i++) {
                    //Send the command to run an epoch
                    Console.WriteLine($"Running Epoch {i} on {numWorkerNodes} worker nodes");
                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH);
                    workers.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                    WaitForWorkerAcksAndReset();
                    Console.WriteLine($"Finished running epoch {i} on {numWorkerNodes} worker nodes");
                }
            }
        }

        static void PullFromWorkers()
        {
            {
                // Task Sink
                // Bindd PULL socket to tcp://localhost:5558
                // Collects results from workers via that socket
                Console.WriteLine("====== SINK ======");

                results = new WorkloadResults[workload.numEpochs,numWorkerNodes];
                //socket to receive results on
                using (var sink = new PullSocket(sinkAddress))
                {
                    for(int i=0;i<numWorkerNodes;i++) {
                        var msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                        Trace.Assert(msg.msgType == Utilities.MsgType.WORKER_CONNECT);
                        ackedWorkers.Signal();

                        msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                        Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT_ACK);
                        ackedWorkers.Signal();
                    }

                    //Wait for epoch acks
                    for(int i=0;i<workload.numEpochs;i++) {
                        for(int j=0;j<numWorkerNodes;j++) {
                            var msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                            Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH_ACK);
                            results[i,j] = Helper.deserializeFromByteArray<WorkloadResults>(msg.contents);
                            ackedWorkers.Signal();
                        }                        
                    }
                }
            }
        }

        private static async void InitiateClientAndSpawnConfigurationCoordinator()
        {
            //Spawn the configuration grain
            if(client == null)
            {
                ClientConfiguration config = new ClientConfiguration();
                
                if (LocalCluster)
                    client = await config.StartClientWithRetries();
                else
                    client = await config.StartClientWithRetriesToCluster();
            }

            if(configGrain == null && workload.grainImplementationType == ImplementationType.SNAPPER) {
                configGrain = client.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
                await configGrain.UpdateNewConfiguration(exeConfig);
                await configGrain.UpdateNewConfiguration(coordConfig);
                Console.WriteLine("Spawned the configuration grain.");
            }
            asyncInitializationDone = true;
        }        

        private static async void LoadGrains()
        {
            var tasks = new List<Task<FunctionResult>>(); 
            var batchSize = -1; //If you want to load the grains in sequence instead of all concurrent
            for(uint i=0; i<workload.numAccounts/workload.numAccountsPerGroup; i++)
            {
                var args = new Tuple<uint, uint>(workload.numAccountsPerGroup, i);
                var input = new FunctionInput(args);
                var groupGUID = Helper.convertUInt32ToGuid(i);
                switch (workload.grainImplementationType) {
                    case ImplementationType.ORLEANSEVENTUAL: 
                        var etxnGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(groupGUID);
                        tasks.Add(etxnGrain.StartTransaction("InitBankAccounts", input));
                        break;
                    case ImplementationType.ORLEANSTXN:
                        var orltxnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(groupGUID);
                        tasks.Add(orltxnGrain.StartTransaction("InitBankAccounts", input));
                        break;
                    case ImplementationType.SNAPPER:
                        var sntxnGrain = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                        tasks.Add(sntxnGrain.StartTransaction("InitBankAccounts", input));
                        break;
                    default:
                        throw new Exception("Unknown grain implementation type");
                }
                if(batchSize > 0 && (i+1)%batchSize == 0) {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            if(tasks.Count > 0) {
                await Task.WhenAll(tasks);
            }
            loadingDone = true;
        }

        private static void GetWorkloadSettings() {
            //Console.WriteLine("Enter number of worker nodes");
            //var numNodes = Console.Read();
            //numWorkerNodes = Convert.ToInt32(numNodes);
            ackedWorkers = new CountdownEvent(numWorkerNodes);            
            workload = new WorkloadConfiguration();
            GenerateWorkLoadFromSettingsFile();
        }
        static void Main(string[] args)
        {
            //Generate workload configurations interactively            
            GetWorkloadSettings();

            //Initialize the client to silo cluster, create configurator grain
            InitiateClientAndSpawnConfigurationCoordinator();
            while (!asyncInitializationDone)
                Thread.Sleep(100);

            //Create the workload grains, load with data
            LoadGrains();
            while(!loadingDone)
            {
                Thread.Sleep(100);
            }

            //Start the controller thread
            Thread conducterThread = new Thread(PushToWorkers);
            conducterThread.Start();
            
            //Start the sink thread
            Thread sinkThread = new Thread(PullFromWorkers);
            sinkThread.Start();

            //Wait for the threads to exit
            conducterThread.Join();
            sinkThread.Join();

            Console.WriteLine("Aggregating results and printing");
            AggregateResultsAndPrint();
            Console.WriteLine("Finished running experiment. Press Enter to exit");
            Console.ReadLine();
        }
    }
}
