using NetMQ;
using System;
using Utilities;
using NetMQ.Sockets;
using System.Threading;
using System.Diagnostics;
using System.Configuration;
using System.Collections.Specialized;
using ExperimentProcess;
using MessagePack;

namespace ExperimentController
{
    static class Program
    {
        // for communication between ExpController and ExpProcess
        static string sinkAddress;
        static string workerAddress;
        //static ISerializer serializer;
        static CountdownEvent ackedWorkers;

        static WorkloadConfiguration workload;

        static long[] IOCount;
        static ServerConnector serverConnector;
        static ExperimentResultAggregator resultAggregator;

        static void Main()
        {
            GenerateWorkLoadFromSettingsFile();
            Console.WriteLine($"silo CPU = {Constants.numCPUPerSilo}, detPercent = {workload.pactPercent}%");

            IOCount = new long[workload.numEpochs];
            serverConnector = new ServerConnector(
                workload.numEpochs,
                workload.benchmark,
                IOCount);
            resultAggregator = new ExperimentResultAggregator(
                workload.pactPercent,
                workload.numEpochs,
                workload.numWarmupEpoch,
                IOCount);

            serverConnector.InitiateClientAndServer();
            serverConnector.LoadGrains();
            
            SetUpExpProcessCommunication();
            //Start the controller thread
            Thread conducterThread = new Thread(PushToWorkers);
            conducterThread.Start();

            //Start the sink thread
            Thread sinkThread = new Thread(PullFromWorkers);
            sinkThread.Start();

            //Wait for the threads to exit
            sinkThread.Join();
            conducterThread.Join();

            resultAggregator.AggregateResultsAndPrint();

            Console.WriteLine("Finished running experiment. Press Enter to exit");
            //Console.ReadLine();
        }

        static void GenerateWorkLoadFromSettingsFile()
        {
            workload = new WorkloadConfiguration();

            // Parse and initialize benchmarkframework section
            var benchmarkFrameWorkSection = ConfigurationManager.GetSection("BenchmarkFrameworkConfig") as NameValueCollection;
            workload.numEpochs = int.Parse(benchmarkFrameWorkSection["numEpoch"]);
            workload.numWarmupEpoch = int.Parse(benchmarkFrameWorkSection["numWarmupEpoch"]);
            workload.epochDurationMSecs = int.Parse(benchmarkFrameWorkSection["epochDurationMSecs"]);

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

        static void SetUpExpProcessCommunication()
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

            //serializer = new MsgPackSerializer();
            ackedWorkers = new CountdownEvent(Constants.numWorker);
        }

        static void WaitForWorkerAcksAndReset()
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
                var msg = new NetworkMessage(Utilities.MsgType.WORKLOAD_INIT, MessagePackSerializer.Serialize(workload));
                workers.SendMoreFrame("WORKLOAD_INIT").SendFrame(MessagePackSerializer.Serialize(msg));
                Console.WriteLine($"Coordinator waits for WORKLOAD_INIT_ACK");
                //Wait for acks for the workload configuration
                WaitForWorkerAcksAndReset();
                Console.WriteLine($"Receive workload configuration ack from {Constants.numWorker} worker nodes");

                for (int i = 0; i < workload.numEpochs; i++)
                {
                    serverConnector.SetIOCount();

                    //Send the command to run an epoch
                    Console.WriteLine($"Running Epoch {i} on {Constants.numWorker} worker nodes");
                    msg = new NetworkMessage(Utilities.MsgType.RUN_EPOCH);
                    workers.SendMoreFrame("RUN_EPOCH").SendFrame(MessagePackSerializer.Serialize(msg));
                    WaitForWorkerAcksAndReset();
                    Console.WriteLine($"Finished running epoch {i} on {Constants.numWorker} worker nodes");

                    serverConnector.GetIOCount(i);
                    serverConnector.ResetOrderGrain();
                    serverConnector.CheckGC();
                }
            }
        }

        static void PullFromWorkers()
        {
            // Task Sink
            // Bindd PULL socket to tcp://localhost:5558
            // Collects results from workers via that socket
            Console.WriteLine("====== PULL FROM WORKERS ======");

            //socket to receive results on
            using (var sink = new PullSocket(sinkAddress))
            {
                for (int i = 0; i < Constants.numWorker; i++)
                {
                    var msg = MessagePackSerializer.Deserialize<NetworkMessage>(sink.ReceiveFrameBytes());
                    Trace.Assert(msg.msgType == Utilities.MsgType.WORKER_CONNECT);
                    Console.WriteLine($"Receive WORKER_CONNECT from worker {i}");
                    ackedWorkers.Signal();
                }

                for (int i = 0; i < Constants.numWorker; i++)
                {
                    var msg = MessagePackSerializer.Deserialize<NetworkMessage>(sink.ReceiveFrameBytes());
                    Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT_ACK);
                    Console.WriteLine($"Receive WORKLOAD_INIT_ACT from worker {i}");
                    ackedWorkers.Signal();
                }

                //Wait for epoch acks
                for (int i = 0; i < workload.numEpochs; i++)
                {
                    for (int j = 0; j < Constants.numWorker; j++)
                    {
                        var msg = MessagePackSerializer.Deserialize<NetworkMessage>(sink.ReceiveFrameBytes());
                        Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH_ACK);
                        var result = MessagePackSerializer.Deserialize<WorkloadResult>(msg.content);
                        resultAggregator.SetResult(i, j, result);
                        ackedWorkers.Signal();
                    }
                }
            }
        }
    }
}