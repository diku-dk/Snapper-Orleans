using System;
using Utilities;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using ExperimentProcess;
using System.Diagnostics;
using SmallBank.Interfaces;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using System.Threading;
using NetMQ.Sockets;
using NetMQ;

namespace MyController
{
    class Program
    {
        static int numWorker = 1;
        static int numCoord = 200;
        static Boolean LocalCluster = true;
        static IClusterClient client;
        static WorkloadConfiguration config;

        static String workerAddress = "@tcp://localhost:5575";
        static String sinkAddress = "@tcp://localhost:5558";
        static CountdownEvent ackedWorkers = new CountdownEvent(numWorker);

        static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            // initialize configuration
            config = new WorkloadConfiguration();
            config.numWorkerNodes = numWorker;
            config.numConnToClusterPerWorkerNode = 1;
            config.numThreadsPerWorkerNode = 4;
            config.numEpochs = 1;
            config.epochDurationMSecs = 30000;
            config.benchmark = BenchmarkType.SMALLBANK;
            config.deterministicTxnPercent = 100;
            config.distribution = Distribution.UNIFORM;
            config.zipfianConstant = 0;
            config.mixture = new int[5];
            config.mixture[0] = 0;
            config.mixture[1] = 0;
            config.mixture[2] = 100;
            config.mixture[3] = 0;
            config.mixture[4] = 0;
            config.numAccounts = 200;
            config.numAccountsMultiTransfer = 32;
            config.numAccountsPerGroup = 1;
            config.numGrainsMultiTransfer = 4;
            config.grainImplementationType = ImplementationType.SNAPPER;

            // initialize clients
            ClientConfiguration clientConfig = new ClientConfiguration();
            if (LocalCluster) client = await clientConfig.StartClientWithRetries();
            else client = await clientConfig.StartClientWithRetriesToCluster();

            // initialize configuration grain
            Console.WriteLine($"Initializing configuration grain...");
            var nonDetCCType = ConcurrencyType.S2PL;
            var maxNonDetWaitingLatencyInMSecs = 1000;
            var batchIntervalMSecs = 100;
            var backoffIntervalMsecs = 10000;
            var idleIntervalTillBackOffSecs = 120;
            var numCoordinators = (uint)numCoord;
            var exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), maxNonDetWaitingLatencyInMSecs);
            var coordConfig = new CoordinatorGrainConfiguration(batchIntervalMSecs, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numCoordinators);
            var configGrain = client.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
            await configGrain.UpdateNewConfiguration(exeConfig);
            await configGrain.UpdateNewConfiguration(coordConfig);

            /*
            // load grains
            Console.WriteLine($"Loading grains...");
            var tasks = new List<Task<FunctionResult>>();
            for (uint i = 0; i < config.numAccounts / config.numAccountsPerGroup; i++)
            {
                var args = new Tuple<uint, uint>(config.numAccountsPerGroup, i);
                var input = new FunctionInput(args);
                var groupGUID = Helper.convertUInt32ToGuid(i);
                var grain = clients[i % numClient].GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                //var grain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(groupGUID);
                tasks.Add(grain.StartTransaction("InitBankAccounts", input));
            }
            await Task.WhenAll(tasks);*/

            //Start the controller thread
            Thread conducterThread = new Thread(PushToWorkers);
            conducterThread.Start();

            //Start the sink thread
            Thread sinkThread = new Thread(PullFromWorkers);
            sinkThread.Start();

            //Wait for the threads to exit
            sinkThread.Join();
            conducterThread.Join();

            Console.WriteLine("Finished running experiment. Press Enter to exit");
            Console.ReadLine();
            return 0;
        }

        private static void WaitForWorkerAcksAndReset()
        {
            ackedWorkers.Wait();
            ackedWorkers.Reset(numWorker); //Reset for next ack, not thread-safe but provides visibility, ok for us to use due to lock-stepped (distributed producer/consumer) usage pattern i.e., Reset will never called concurrently with other functions (Signal/Wait)            
        }

        static void PushToWorkers()
        {
            using (var workers = new PublisherSocket(workerAddress))
            {
                WaitForWorkerAcksAndReset();
                Console.WriteLine($"{numWorker} worker nodes have connected to Controller");
                Console.WriteLine($"Sent workload configuration to {numWorker} worker nodes");
                var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT);
                msg.contents = Helper.serializeToByteArray<WorkloadConfiguration>(config);
                workers.SendMoreFrame("WORKLOAD_INIT").SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                Console.WriteLine($"Coordinator waits for WORKLOAD_INIT_ACK");
                //Wait for acks for the workload configuration
                WaitForWorkerAcksAndReset();
                Console.WriteLine($"Receive workload configuration ack from {numWorker} worker nodes");

                for (int i = 0; i < config.numEpochs; i++)
                {
                    //Send the command to run an epoch
                    Console.WriteLine($"Running Epoch {i} on {numWorker} worker nodes");
                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH);
                    workers.SendMoreFrame("RUN_EPOCH").SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                    WaitForWorkerAcksAndReset();
                    Console.WriteLine($"Finished running epoch {i} on {numWorker} worker nodes");
                }
            }
        }

        static void PullFromWorkers()
        {
            {
                using (var sink = new PullSocket(sinkAddress))
                {
                    for (int i = 0; i < numWorker; i++)
                    {
                        var msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                        Trace.Assert(msg.msgType == Utilities.MsgType.WORKER_CONNECT);
                        Console.WriteLine($"Receive WORKER_CONNECT from worker {i}");
                        ackedWorkers.Signal();
                    }

                    for (int i = 0; i < numWorker; i++)
                    {
                        var msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                        Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT_ACK);
                        Console.WriteLine($"Receive WORKLOAD_INIT_ACT from worker {i}");
                        ackedWorkers.Signal();
                    }

                    //Wait for epoch acks
                    for (int i = 0; i < config.numEpochs; i++)
                    {
                        for (int j = 0; j < numWorker; j++)
                        {
                            var msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                            Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH_ACK);
                            ackedWorkers.Signal();
                        }
                    }
                }
            }
        }
    }
}