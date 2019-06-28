using System;
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

namespace ExperimentConductor
{
    class Program
    {
        static String workerAddress = "@tcp://localhost:5575";
        static String sinkAddress = ">tcp://localhost:5558";
        static int numWorkerNodes = 1; 
        static IClusterClient client;
        static Boolean LocalCluster = false;
        static IConfigurationManagerGrain configGrain;
        static bool asyncInitializationDone = false;
        static CountdownEvent ackedWorkers;
        static WorkloadConfiguration workload;
        static ExecutionGrainConfiguration exeConfig;
        static CoordinatorGrainConfiguration coordConfig;

        private static void AggregateResultsAndPrint() {
            //TODO aggregation and printing/saving of experimental results
        }
        private static void WaitForWorkerAcksAndReset() {
                ackedWorkers.Wait();
                ackedWorkers = new CountdownEvent(numWorkerNodes); //Reset for next ack
        }
        static void PushToWorkers() {
            // Task Ventilator
            // Binds PUSH socket to tcp://localhost:5557
            // Sends batch of tasks to workers via that socket
            Console.WriteLine("====== VENTILATOR ======");
            using (var workers = new PushSocket(workerAddress))
            {                
                //Wait for the workers to connect to conductor
                WaitForWorkerAcksAndReset();
                //Send the workload configuration
                Console.WriteLine($"{numWorkerNodes} worker nodes have connected to Conductor");
                var netMessage = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT);
                netMessage.contents = Helper.serializeToByteArray<WorkloadConfiguration>(workload);
                workers.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(netMessage));
                Console.WriteLine("Sent workload configuration to workers");
                //Wait for acks for the workload configuration
                WaitForWorkerAcksAndReset();

                for(int i=0;i<workload.numEpochs;i++) {
                    //Send the command to run an epoch
                    Console.WriteLine($"Running Epoch {i} on worker nodes");
                    var msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH);
                    workers.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(netMessage));
                    WaitForWorkerAcksAndReset();
                    Console.WriteLine($"Finished running epoch {i} on worker nodes");
                }

                Console.WriteLine("Aggregating results");
                AggregateResultsAndPrint();
                Console.WriteLine("Finished aggregating results, exiting");
            }
        }

        static void PullFromWorkers()
        {
            {
                // Task Sink
                // Bindd PULL socket to tcp://localhost:5558
                // Collects results from workers via that socket
                Console.WriteLine("====== SINK ======");

                WorkloadResults[,] results = new WorkloadResults[workload.numEpochs,numWorkerNodes];
                //socket to receive results on
                using (var sink = new PullSocket(sinkAddress))
                {
                    for(int i=0;i<numWorkerNodes;i++) {
                        var msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                        Trace.Assert(msg.msgType == Utilities.MsgType.WORKER_CONNECT);
                        ackedWorkers.Signal();
                    }

                    //Wait for epoch acks
                    for(int i=0;i<workload.numEpochs;i++) {
                        for(int j=0;i<numWorkerNodes;j++) {
                            var msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                            Trace.Assert(msg.msgType != Utilities.MsgType.RUN_EPOCH_ACK);
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

        private static void GenerateWorkLoad()
        {
            workload.numWorkerNodes = numWorkerNodes;
            workload.numThreadsPerWorkerNode = 16;
            workload.epochInMiliseconds = 10000;
            workload.numEpochs = 2;
            workload.asyncMsgSizePerThread = 1000;
            
            workload.benchmark = BenchmarkType.SMALLBANK;
            workload.distribution = Distribution.UNIFORM;
            workload.numAccounts = 10000;
            workload.numAccountsPerGroup = 10;
            //workload.mixture = new int[6] { 15, 5, 45, 10, 5, 20 };//{getBalance, depositChecking, transder,transacSaving, writeCheck, multiTransfer}
            workload.mixture = new int[6] { 100, 0, 0, 0, 0, 0 };//{getBalance, depositChecking, transder, transacSaving, writeCheck, multiTransfer}
            workload.numAccountsMultiTransfer = 32;
            workload.numGrainsMultiTransfer = 4;
            workload.zipf = 1;
            workload.deterministicTxnPercent = 0;
            workload.grainImplementationType = ImplementationType.ORLEANSEVENTUAL;


            var nonDetCCType = ConcurrencyType.TIMESTAMP;
            int maxNonDetWaitingLatencyInMs = 10000;
            int batchIntervalMsecs = 1000;
            int backoffIntervalMsecs = 10000;
            int idleIntervalTillBackOffSecs = 30000;
            uint numOfCoordinators = 5;

            exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), maxNonDetWaitingLatencyInMs);
            coordConfig = new CoordinatorGrainConfiguration(batchIntervalMsecs, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numOfCoordinators);
            Console.WriteLine("Generated workload configuration");
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
        }

        private static void GetWorkloadSettings() {
            //Console.WriteLine("Enter number of worker nodes");
            //var numNodes = Console.Read();
            //numWorkerNodes = Convert.ToInt32(numNodes);
            ackedWorkers = new CountdownEvent(numWorkerNodes);            
            workload = new WorkloadConfiguration();
            GenerateWorkLoad();
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

            //Start the conductor thread
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
