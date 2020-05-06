using System;
using NetMQ.Sockets;
using NetMQ;
using System.Threading;
using System.Diagnostics;
using Utilities;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using ExperimentProcess;

namespace MyProcess
{
    class Program
    {
        static int workerID = 1;
        static int global_tid = 200 + workerID;

        static Boolean LocalCluster = true;
        static String sinkAddress = ">tcp://localhost:5558";
        static String controllerAddress = ">tcp://localhost:5575";
        static PushSocket sink = new PushSocket(sinkAddress);
        static WorkloadConfiguration config;

        static IClusterClient[] clients;
        static IBenchmark[] benchmarks;
        static Barrier[] barriers;
        static CountdownEvent[] threadAcks;
        static bool initializationDone = false;
        static Thread[] threads;

        static void Main(string[] args)
        {
            Console.WriteLine("My Worker is Started...");
            ProcessWork();
        }

        static void ProcessWork()
        {
            using (var controller = new SubscriberSocket(controllerAddress))
            {
                controller.Subscribe("WORKLOAD_INIT");
                //Acknowledge the controller thread
                var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKER_CONNECT);
                sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                Console.WriteLine("Connected to controller");

                controller.Options.ReceiveHighWatermark = 1000;
                var messageTopicReceived = controller.ReceiveFrameString();
                var messageReceived = controller.ReceiveFrameBytes();
                //Wait to receive workload msg
                msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(messageReceived);
                Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT);
                Console.WriteLine("Receive workload configuration.");
                controller.Unsubscribe("WORKLOAD_INIT");
                controller.Subscribe("RUN_EPOCH");
                config = Helper.deserializeFromByteArray<WorkloadConfiguration>(msg.contents);
                Console.WriteLine("Received workload message from controller");

                //Initialize threads and other data-structures for epoch runs
                Initialize();
                while (!initializationDone)
                    Thread.Sleep(100);

                Console.WriteLine("Finished initialization, sending ACK to controller");
                //Send an ACK
                msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT_ACK);
                sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));

                for (int i = 0; i < config.numEpochs; i++)
                {
                    messageTopicReceived = controller.ReceiveFrameString();
                    messageReceived = controller.ReceiveFrameBytes();
                    //Wait for EPOCH RUN signal
                    msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(messageReceived);
                    Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH);
                    Console.WriteLine($"Received signal from controller. Running epoch {i} across {config.numThreadsPerWorkerNode} worker threads");
                    //Signal the barrier
                    barriers[i].SignalAndWait();
                    //Wait for all threads to finish the epoch
                    threadAcks[i].Wait();
                    var result = new WorkloadResults(1,1,1,1,new List<double>(), null);
                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH_ACK);
                    msg.contents = Helper.serializeToByteArray<WorkloadResults>(result);
                    sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                }

                Console.WriteLine("Finished running epochs, exiting");
                foreach (var thread in threads)
                {
                    thread.Join();
                }
            }
        }

        private static async void Initialize()
        {
            benchmarks = new SmallBankBenchmark[config.numThreadsPerWorkerNode];

            for (int i = 0; i < config.numThreadsPerWorkerNode; i++)
            {
                switch (config.benchmark)
                {
                    case BenchmarkType.SMALLBANK:
                        benchmarks[i] = new SmallBankBenchmark();
                        benchmarks[i].generateBenchmark(config, i);
                        break;
                    default:
                        throw new Exception("Unknown benchmark type");
                }
            }

            await InitializeClients();
            InitializeThreads();
            initializationDone = true;
        }

        private static void InitializeThreads()
        {
            barriers = new Barrier[config.numEpochs];
            threadAcks = new CountdownEvent[config.numEpochs];
            for (int i = 0; i < config.numEpochs; i++)
            {
                barriers[i] = new Barrier(config.numThreadsPerWorkerNode + 1);
                threadAcks[i] = new CountdownEvent(config.numThreadsPerWorkerNode);
            }

            //Spawn Threads        
            threads = new Thread[config.numThreadsPerWorkerNode];
            for (int i = 0; i < config.numThreadsPerWorkerNode; i++)
            {
                int threadIndex = i;
                Thread thread = new Thread(ThreadWorkAsync);
                threads[threadIndex] = thread;
                thread.Start(threadIndex);
            }
        }

        private static async Task InitializeClients()
        {
            clients = new IClusterClient[config.numConnToClusterPerWorkerNode];
            ClientConfiguration clientConfig = new ClientConfiguration();
            for (int i = 0; i < config.numConnToClusterPerWorkerNode; i++)
            {
                if (LocalCluster) clients[i] = await clientConfig.StartClientWithRetries();
                else clients[i] = await clientConfig.StartClientWithRetriesToCluster();
            }
        }

        private static async void ThreadWorkAsync(Object obj)
        {
            int threadIndex = (int)obj;
            var globalWatch = new Stopwatch();
            var benchmark = benchmarks[threadIndex];
            var client = clients[threadIndex % config.numConnToClusterPerWorkerNode];
            Console.WriteLine("get into ThreadWorkAsync()");
            for (int eIndex = 0; eIndex < config.numEpochs; eIndex++)
            {
                barriers[eIndex].SignalAndWait();
                //var t = new List<Task<TransactionContext>>();
                var t = new List<Task<FunctionResult>>();
                globalWatch.Restart();
                var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                do
                {
                    for (int i = 0; i < 100; i++) t.Add(benchmark.newTransaction(client, global_tid++));
                    await Task.Delay(5);
                }
                while (globalWatch.ElapsedMilliseconds < config.epochDurationMSecs);
                Console.WriteLine($"Txn emit rate: {t.Count * 1000 / globalWatch.ElapsedMilliseconds} txn per second. ");
                try
                {
                    await Task.WhenAll(t);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Exception 1: {e.Message}");
                }
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                var commit = 0;
                try
                {
                    for (int i = 0; i < t.Count; i++)
                    {
                        if (t[i].Result.exception == false) commit++;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"Exception 2: {e.Message}");
                }
                Console.WriteLine($"numTxn = {t.Count}, time = {endTime - startTime}, commit = {commit}, tp = {1000 * commit / (endTime - startTime)}");
                Console.ReadLine();
                globalWatch.Stop();
                //Signal the completion of epoch
                threadAcks[eIndex].Signal();
                Console.ReadLine();
            }
        }
    }
}






