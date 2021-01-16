using NetMQ;
using System;
using Orleans;
using Utilities;
using NetMQ.Sockets;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace ExperimentProcess
{
    class Program
    {
        static string sinkAddress;
        static string controllerAddress;
        static IClusterClient[] clients;
        static PushSocket sink;
        static Thread[] threads;
        static Barrier[] barriers;
        static ISerializer serializer;
        static IBenchmark[] benchmarks;
        static WorkloadResults[] results;
        static CountdownEvent[] threadAcks;
        static WorkloadConfiguration config;
        static bool initializationDone = false;

        static bool isDet;
        static int siloCPU;
        static int pipeSize;
        static int numWorkerThread;

        private static async void ThreadWorkAsync(object obj)
        {
            int threadIndex = (int)obj;
            var globalWatch = new Stopwatch();
            var benchmark = benchmarks[threadIndex];
            var client = clients[threadIndex];
            Console.WriteLine($"thread = {threadIndex}, pipe = {pipeSize}");
            for (int eIndex = 0; eIndex < config.numEpochs; eIndex++)
            {
                int numEmit = 0;
                int numDetCommit = 0;
                int numNonDetCommit = 0;
                int numDetTransaction = 0;
                int numNonDetTransaction = 0;
                int numDeadlock = 0;
                int numNotSerializable = 0;
                var latencies = new List<double>();
                var det_latencies = new List<double>();
                var tasks = new List<Task<TransactionResult>>();
                var reqs = new Dictionary<Task<TransactionResult>, TimeSpan>();
                await Task.Delay(TimeSpan.FromMilliseconds(100));   // give some time for producer to populate the buffer
                //Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                globalWatch.Restart();
                var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                do
                {
                    while (tasks.Count < pipeSize)
                    {
                        var asyncReqStartTime = globalWatch.Elapsed;
                        var newTask = benchmark.newTransaction(client, threadIndex);
                        numEmit++;
                        reqs.Add(newTask, asyncReqStartTime);
                        tasks.Add(newTask);
                    }
                    if (tasks.Count != 0)
                    {
                        var task = await Task.WhenAny(tasks);
                        var asyncReqEndTime = globalWatch.Elapsed;
                        bool noException = true;
                        try
                        {
                            //Needed to catch exception of individual task (not caught by Snapper's exception) which would not be thrown by WhenAny
                            await task;
                        }
                        catch (Exception e)    // this exception is only related to OrleansTransaction
                        {
                            Console.WriteLine($"Exception:{e.Message}, {e.StackTrace}");
                            noException = false;
                        }
                        if (noException)
                        {
                            if (task.Result.isDet)   // for det
                            {
                                numDetTransaction++;
                                if (!task.Result.exception)
                                {
                                    numDetCommit++;
                                    det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                                }
                            }
                            else    // for non-det + eventual + orleans txn
                            {
                                numNonDetTransaction++;
                                if (!task.Result.exception)
                                {
                                    numNonDetCommit++;
                                    latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                                }
                                else if (task.Result.Exp_Serializable) numNotSerializable++;
                                else if (task.Result.Exp_Deadlock) numDeadlock++;
                            }
                        }
                        tasks.Remove(task);
                        reqs.Remove(task);
                    }
                }
                while (globalWatch.ElapsedMilliseconds < config.epochDurationMSecs);
                //Wait for the tasks exceeding epoch time and also count them into results
                while (tasks.Count != 0)
                {
                    var task = await Task.WhenAny(tasks);
                    var asyncReqEndTime = globalWatch.Elapsed;
                    bool noException = true;
                    try
                    {
                        await task;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Exception: {e.Message}. ");
                        noException = false;
                    }
                    if (noException)
                    {
                        if (task.Result.isDet)   // for det
                        {
                            numDetTransaction++;
                            if (!task.Result.exception)
                            {
                                numDetCommit++;
                                det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                            }
                        }
                        else    // for non-det + eventual + orleans txn
                        {
                            numNonDetTransaction++;
                            if (!task.Result.exception)
                            {
                                numNonDetCommit++;
                                latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                            }
                            else if (task.Result.Exp_Serializable) numNotSerializable++;
                            else if (task.Result.Exp_Deadlock) numDeadlock++;
                        }
                    }
                    tasks.Remove(task);
                    reqs.Remove(task);
                }
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                globalWatch.Stop();

                if (isDet) Console.WriteLine($"total_num_det = {numDetTransaction}, det-commit = {numDetCommit}, tp = {1000 * numDetCommit / (endTime - startTime)}. ");
                else Console.WriteLine($"total_num_nondet = {numNonDetTransaction}, nondet-commit = {numNonDetCommit}, tp = {1000 * numNonDetCommit / (endTime - startTime)}, Deadlock = {numDeadlock}, NotSerilizable = {numNotSerializable}");
                WorkloadResults res;
                if (config.grainImplementationType == ImplementationType.SNAPPER)
                    res = new WorkloadResults(numDetTransaction, numNonDetTransaction, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numDeadlock);
                else res = new WorkloadResults(numDetTransaction, numEmit, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numDeadlock);
                res.setLatency(latencies, det_latencies);
                results[threadIndex] = res;
                threadAcks[eIndex].Signal();  //Signal the completion of epoch
            }
        }

        private static async void Initialize()
        {
            if (config.deterministicTxnPercent == 100) isDet = true;
            else if (config.deterministicTxnPercent == 0) isDet = false;
            else throw new Exception($"Exception: ExperimentProcess does not support hybrid");
            numWorkerThread = siloCPU / 2;
            switch (config.benchmark)
            {
                case BenchmarkType.SMALLBANK:
                    benchmarks = new SmallBankBenchmark[numWorkerThread];
                    for (int i = 0; i < numWorkerThread; i++) benchmarks[i] = new SmallBankBenchmark();
                    break;
                case BenchmarkType.TPCC:
                    benchmarks = new TPCCBenchmark[numWorkerThread];
                    for (int i = 0; i < numWorkerThread; i++) benchmarks[i] = new TPCCBenchmark();
                    break;
                default:
                    throw new Exception("Exception: NewProcess only support SmallBank and TPCC benchmarks");
            }
            results = new WorkloadResults[numWorkerThread];
            for (int i = 0; i < numWorkerThread; i++) benchmarks[i].generateBenchmark(config);

            barriers = new Barrier[config.numEpochs];
            threadAcks = new CountdownEvent[config.numEpochs];
            for (int i = 0; i < config.numEpochs; i++)
            {
                barriers[i] = new Barrier(numWorkerThread + 1);
                threadAcks[i] = new CountdownEvent(numWorkerThread);
            }

            await InitializeClients();

            // spawn threads
            threads = new Thread[numWorkerThread];
            for (int i = 0; i < numWorkerThread; i++)
            {
                var thread = new Thread(ThreadWorkAsync);
                threads[i] = thread;
                thread.Start(i);
            }
            initializationDone = true;
        }

        private static async Task InitializeClients()
        {
            clients = new IClusterClient[numWorkerThread];
            var clientConfig = new ClientConfiguration();
            for (int i = 0; i < numWorkerThread; i++)
            {
                if (Constants.localCluster) clients[i] = await clientConfig.StartClientWithRetries();
                else clients[i] = await clientConfig.StartClientWithRetriesToCluster();
            }
        }

        static void ProcessWork()
        {
            Console.WriteLine("====== WORKER ======");
            using (var controller = new SubscriberSocket(controllerAddress))
            {
                Console.WriteLine($"worker is ready to connect controller");
                controller.Subscribe("WORKLOAD_INIT");
                //Acknowledge the controller thread
                var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKER_CONNECT);
                sink.SendFrame(serializer.serialize(msg));
                Console.WriteLine("Connected to controller");

                controller.Options.ReceiveHighWatermark = 1000;
                var messageTopicReceived = controller.ReceiveFrameString();
                var messageReceived = controller.ReceiveFrameBytes();
                //Wait to receive workload msg
                msg = serializer.deserialize<NetworkMessageWrapper>(messageReceived);
                Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT);
                Console.WriteLine("Receive workload configuration.");
                controller.Unsubscribe("WORKLOAD_INIT");
                controller.Subscribe("RUN_EPOCH");
                config = serializer.deserialize<WorkloadConfiguration>(msg.contents);
                Console.WriteLine("Received workload message from controller");

                //Initialize threads and other data-structures for epoch runs
                Initialize();
                while (!initializationDone) Thread.Sleep(100);

                Console.WriteLine("Finished initialization, sending ACK to controller");
                //Send an ACK
                msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT_ACK);
                sink.SendFrame(serializer.serialize(msg));

                for (int i = 0; i < config.numEpochs; i++)
                {
                    messageTopicReceived = controller.ReceiveFrameString();
                    messageReceived = controller.ReceiveFrameBytes();
                    //Wait for EPOCH RUN signal
                    msg = serializer.deserialize<NetworkMessageWrapper>(messageReceived);
                    Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH);
                    Console.WriteLine($"Received signal from controller. Running epoch {i} across {numWorkerThread} worker threads");
                    //Signal the barrier
                    barriers[i].SignalAndWait();
                    //Wait for all threads to finish the epoch
                    threadAcks[i].Wait();
                    var result = AggregateAcrossThreadsForEpoch();
                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH_ACK);
                    msg.contents = serializer.serialize(result);
                    sink.SendFrame(serializer.serialize(msg));
                }

                Console.WriteLine("Finished running epochs, exiting");
                foreach (var thread in threads) thread.Join();
            }
        }

        private static WorkloadResults AggregateAcrossThreadsForEpoch()
        {
            Trace.Assert(results.Length >= 1);
            int aggNumDetCommitted = results[0].numDetCommitted;
            int aggNumNonDetCommitted = results[0].numNonDetCommitted;
            int aggNumDetTransactions = results[0].numDetTxn;
            int aggNumNonDetTransactions = results[0].numNonDetTxn;
            int aggNumNotSerializable = results[0].numNotSerializable;
            int aggNumDeadlock = results[0].numDeadlock;
            long aggStartTime = results[0].startTime;
            long aggEndTime = results[0].endTime;
            var aggLatencies = new List<double>();
            var aggDetLatencies = new List<double>();
            aggLatencies.AddRange(results[0].latencies);
            aggDetLatencies.AddRange(results[0].det_latencies);
            for (int i = 1; i < results.Length; i++)    // reach thread has a result
            {
                aggNumDetCommitted += results[i].numDetCommitted;
                aggNumNonDetCommitted += results[i].numNonDetCommitted;
                aggNumDetTransactions += results[i].numDetTxn;
                aggNumNonDetTransactions += results[i].numNonDetTxn;
                aggNumNotSerializable += results[i].numNotSerializable;
                aggNumDeadlock += results[i].numDeadlock;
                aggStartTime = (results[i].startTime < aggStartTime) ? results[i].startTime : aggStartTime;
                aggEndTime = (results[i].endTime < aggEndTime) ? results[i].endTime : aggEndTime;
                aggLatencies.AddRange(results[i].latencies);
                aggDetLatencies.AddRange(results[i].det_latencies);
            }
            var res = new WorkloadResults(aggNumDetTransactions, aggNumNonDetTransactions, aggNumDetCommitted, aggNumNonDetCommitted, aggStartTime, aggEndTime, aggNumNotSerializable, aggNumDeadlock);
            res.setLatency(aggLatencies, aggDetLatencies);
            return res;
        }

        static void Main(string[] args)
        {
            if (Constants.multiWorker)
            {
                sinkAddress = Constants.worker_Remote_SinkAddress;
                controllerAddress = Constants.worker_Remote_ControllerAddress;
            }
            else
            {
                sinkAddress = Constants.worker_Local_SinkAddress;
                controllerAddress = Constants.worker_Local_ControllerAddress;
            }
            sink = new PushSocket(sinkAddress);
            serializer = new BinarySerializer();

            Console.WriteLine("Worker is Started...");

            //inject the specially required arguments into workload setting
            siloCPU = int.Parse(args[0]);
            pipeSize = int.Parse(args[1]);
            Console.WriteLine($"pipe per thread = {pipeSize}");

            ProcessWork();
            //Console.ReadLine();
        }
    }
}