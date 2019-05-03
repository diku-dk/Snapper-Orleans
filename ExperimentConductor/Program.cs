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
        static int numOfWorkers = 1; 
        static int numOfThreadsPerWorker = 2;
        Random rand = new Random();
        static readonly uint numOfCoordinators = 5;
        static readonly int maxAccounts = 100;
        static readonly int batchIntervalMsecs = 10;
        static readonly int backoffIntervalMsecs = 1000;
        static readonly int idleIntervalTillBackOffSecs = 10;
        readonly int maxTransferAmount = 10;
        readonly int numSequentialTransfers = 10;
        readonly int numConcurrentTransfers = 1000;
        static readonly int maxNonDetWaitingLatencyInMs = 1000;
        static readonly ConcurrencyType nonDetCCType = ConcurrencyType.S2PL;

        static IClusterClient client;
        static Boolean LocalCluster = true;
        static IConfigurationManagerGrain configGrain;
        static bool configGrainUp = false;

        static void PushToWorkers(Object obj) {
            // Task Ventilator
            // Binds PUSH socket to tcp://localhost:5557
            // Sends batch of tasks to workers via that socket
            Console.WriteLine("====== VENTILATOR ======");

            WorkloadConfiguration workload = (WorkloadConfiguration)obj;
            using (var workers = new PushSocket(workerAddress))
            {
                //Console.WriteLine("Press enter when worker are ready");
                //Console.ReadLine();

                //the first message it "0" and signals start of batch
                //see the Sink.csproj Program.cs file for where this is used
                
                var netMessage = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_CONFIG);
                netMessage.contents = Helper.serializeToByteArray<WorkloadConfiguration>(workload);
                workers.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(netMessage));
                Console.WriteLine("Sending workload configuration to workers");
            }
        }

        static void PullFromWorkers()
        {
            {
                // Task Sink
                // Bindd PULL socket to tcp://localhost:5558
                // Collects results from workers via that socket
                Console.WriteLine("====== SINK ======");

                WorkloadResults[] results = new WorkloadResults[numOfWorkers * numOfThreadsPerWorker];
                //socket to receive results on
                using (var sink = new PullSocket(sinkAddress))
                {
                    for (int taskNumber = 0; taskNumber < numOfWorkers; taskNumber++)
                    {
                        var resultMsg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                        //Parse the workloadResult
                        Debug.Assert(resultMsg.msgType == Utilities.MsgType.AGGREGATED_WORKLOAD_RESULTS);
                        AggregatedWorkloadResults result = Helper.deserializeFromByteArray<AggregatedWorkloadResults>(resultMsg.contents);
                   
                        Console.WriteLine($"{result.results.Count} threads reported results.\n");
                    }
                    Console.ReadLine();
                    //Calculate and report the results
                }
                
            }
        }

        private static async void SpawnConfigurationCoordinator()
        {
            //Spawn the configuration grain
            if(configGrain == null)
            {
                ClientConfiguration config = new ClientConfiguration();
                
                if (LocalCluster)
                    client = await config.StartClientWithRetries();
                else
                    client = await config.StartClientWithRetriesToCluster();
                configGrain = client.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
                var exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), maxNonDetWaitingLatencyInMs);
                var coordConfig = new CoordinatorGrainConfiguration(batchIntervalMsecs, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numOfCoordinators);
                await configGrain.UpdateNewConfiguration(exeConfig);
                await configGrain.UpdateNewConfiguration(coordConfig);
                Console.WriteLine("Spawned the configuration grain.");
                configGrainUp = true;
            }
        }

        private static void GenerateWorkLoad(WorkloadConfiguration workload)
        {
            workload.numWorkerNodes = 1;
            workload.numThreadsPerWorkerNodes = 2;
            workload.epochInMiliseconds = 2000;
            workload.numEpoch = 5;
            workload.benchmark = BenchmarkType.SMALLBANK;
            workload.distribution = Distribution.UNIFORM;

            workload.numGroups = 100;
            workload.numAccounts = 10000;
            workload.numAccountsPerGroup = 100;
            workload.mixture = new int[6] { 15, 5, 45, 10, 5, 20 };//{getBalance, depositChecking, transder, transacSaving, writeCheck, multiTransfer}
            workload.numAccountsMultiTransfer = 32;
            workload.numGrainsMultiTransfer = 4;
            workload.zipf = 1;
            workload.deterministicTxnPercent = 50;
            LoadGrains(workload);
            Console.WriteLine("Generated workload configuration");
        }

        private static async void LoadGrains(WorkloadConfiguration workload)
        {
            var tasks = new List<Task<FunctionResult>>(); 
            for(uint i=0; i<workload.numGroups; i++)
            {
                var args = new Tuple<uint, uint>(workload.numAccountsPerGroup, i);
                var input = new FunctionInput(args);
                var groupGUID = Helper.convertUInt32ToGuid(i);
                var destination = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                tasks.Add(destination.StartTransaction("InitBankAccounts", input));
            }
            await Task.WhenAll(tasks);
        }

        static void Main(string[] args)
        {
            //Nothing much
            if(args.Length > 0)
            {
                workerAddress = args[0];
                sinkAddress = args[1];
            }

           
            SpawnConfigurationCoordinator();
            var workLoad = new WorkloadConfiguration();
            while (!configGrainUp)
                Thread.Sleep(100);
            GenerateWorkLoad(workLoad);

            Thread conducterThread = new Thread(PushToWorkers);
            conducterThread.Start(workLoad);
            
            Thread sinkThread = new Thread(PullFromWorkers);
            sinkThread.Start();

            conducterThread.Join();
            sinkThread.Join();
        }
    }
}
