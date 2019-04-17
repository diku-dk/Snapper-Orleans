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
        static readonly int batchIntervalMsecs = 1000;
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

        static void PushToWorkers(Object obj) {
            // Task Ventilator
            // Binds PUSH socket to tcp://localhost:5557
            // Sends batch of tasks to workers via that socket
            Console.WriteLine("====== VENTILATOR ======");

            WorkloadConfiguration workload = (WorkloadConfiguration)obj;
            using (var workers = new PushSocket(workerAddress))
            {
                Console.WriteLine("Press enter when worker are ready");
                Console.ReadLine();

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
                        Debug.Assert(resultMsg.msgType == Utilities.MsgType.WORKLOAD_RESULTS);
                        results[taskNumber] = Helper.deserializeFromByteArray<WorkloadResults>(resultMsg.contents);
                        var res = results[taskNumber];
                        Console.WriteLine($"{res.numSuccessFulTxns} of {res.numTxns} transactions are committed. Latency: {res.averageLatency}. Throughput: {res.throughput}.\n");
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
                IClusterClient client = null;
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
            }
        }


        static void Main(string[] args)
        {
            //Nothing much
            if(args.Length > 0)
            {
                workerAddress = args[0];
                sinkAddress = args[1];
            }
            var workLoad = new WorkloadConfiguration();
            workLoad.numWorkerNodes = numOfWorkers;
            workLoad.numThreadsPerWorkerNodes = numOfThreadsPerWorker;
            workLoad.totalTransactions = numOfWorkers * numOfThreadsPerWorker * 10;

            //Spawn the configuration coordinator
            Thread spawnThread = new Thread(SpawnConfigurationCoordinator);
            spawnThread.Start();
            //spawnThread.Join();

            Console.WriteLine("Started the conductor thread.");
            Thread conducterThread = new Thread(PushToWorkers);
            conducterThread.Start(workLoad);

            Console.WriteLine("Started the sink thread.");
            Thread sinkThread = new Thread(PullFromWorkers);
            sinkThread.Start();

            conducterThread.Join();
            sinkThread.Join();
        }
    }
}
