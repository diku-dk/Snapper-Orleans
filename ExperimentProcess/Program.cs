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
        static WorkloadResults[] results;
        static IBenchmark[] benchmarks;
        static WorkloadConfiguration config;
        static Barrier[] barriers;
        static CountdownEvent[] threadAcks;
        static bool initializationDone = false;
        static Thread[] threads;
        static ISerializer serializer;

        static int pipeSize = 0;
        static int num_txn = 5000;

        // parameters for hot record solution
        static double skew;
        static double hotGrainRatio;

        private static async void ThreadWorkAsync(Object obj)
        {
            int threadIndex = (int)obj;
            var globalWatch = new Stopwatch();
            var benchmark = benchmarks[threadIndex];
            var client = clients[threadIndex % config.numConnToClusterPerWorkerNode];
            //if (isUniform == false) for (int eIndex = 0; eIndex < config.numEpochs; eIndex++) benchmark.generateGrainIDs(threadIndex, eIndex);
            for (int eIndex = 0; eIndex < config.numEpochs; eIndex++)
            {
                //if (isUniform == false) benchmark.setIndex(0);
                int numEmit = 0;
                int numDetCommit = 0;
                int numNonDetCommit = 0;
                int numDetTransaction = 0;
                int numNonDetTransaction = 0;
                int numDeadlock = 0;
                int numNotSerializable = 0;
                var latencies = new List<double>();
                var networkTime = new List<double>();
                var emitTime = new List<double>();
                var executeTime = new List<double>();
                var waitBatchCommitTime = new List<double>();
                var det_latencies = new List<double>();
                var det_networkTime = new List<double>();
                var det_emitTime = new List<double>();
                var det_waitBatchScheduleTime = new List<double>();
                var det_executeTime = new List<double>();
                var det_waitBatchCommitTime = new List<double>();
                var tasks = new List<Task<TransactionResult>>();
                var reqs = new Dictionary<Task<TransactionResult>, TimeSpan>();
                //Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                globalWatch.Restart();
                var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                do
                {

                    //while (tasks.Count < pipeSize && (isUniform || numEmit < num_txn))
                    while (tasks.Count < pipeSize)
                    {
                        //Pipeline remaining tasks
                        var asyncReqStartTime = globalWatch.Elapsed;
                        var newTask = benchmark.newTransaction(client, eIndex);
                        numEmit++;
                        reqs.Add(newTask, asyncReqStartTime);
                        tasks.Add(newTask);
                    }
                    if (reqs.Count != 0)
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
                            Console.WriteLine($"Exception:{e.Message}");
                            noException = false;
                        }
                        if (noException)
                        {
                            if (task.Result.isDet)   // for det or ORLEANSTXN or ORLEANSEVENTUAL
                            {
                                numDetTransaction++;
                                if (!task.Result.exception)
                                {
                                    numDetCommit++;
                                    det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                                    if (config.grainImplementationType == ImplementationType.SNAPPER)
                                    {
                                        det_emitTime.Add((task.Result.emitTime - task.Result.arriveTime).TotalMilliseconds);
                                        det_waitBatchScheduleTime.Add((task.Result.batchTime - task.Result.arriveTime).TotalMilliseconds);
                                        det_waitBatchCommitTime.Add((task.Result.commitTime - task.Result.finishTime).TotalMilliseconds);
                                        det_networkTime.Add((task.Result.arriveTime - reqs[task] + asyncReqEndTime - task.Result.commitTime).TotalMilliseconds);
                                        double exeTime;
                                        if (task.Result.emitTime > task.Result.batchTime) exeTime = (task.Result.finishTime - task.Result.emitTime).TotalMilliseconds;
                                        else exeTime = (task.Result.finishTime - task.Result.batchTime).TotalMilliseconds;
                                        det_executeTime.Add(exeTime);
                                    }
                                }
                            }
                            else    // for non-det 
                            {
                                numNonDetTransaction++;
                                if (!task.Result.exception)
                                {
                                    numNonDetCommit++;
                                    latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                                    emitTime.Add((task.Result.emitTime - task.Result.arriveTime).TotalMilliseconds);
                                    executeTime.Add((task.Result.finishTime - task.Result.emitTime).TotalMilliseconds);
                                    waitBatchCommitTime.Add((task.Result.commitTime - task.Result.finishTime).TotalMilliseconds);
                                    networkTime.Add((task.Result.arriveTime - reqs[task] + asyncReqEndTime - task.Result.commitTime).TotalMilliseconds);
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
                //while (numEmit < num_txn);

                while (tasks.Count != 0)  //Wait for the tasks exceeding epoch time
                {
                    bool noException = true;
                    var task = await Task.WhenAny(tasks);
                    var asyncReqEndTime = globalWatch.Elapsed;
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
                        if (task.Result.isDet)   // for det + eventual + orleans txn
                        {
                            numDetTransaction++;
                            if (!task.Result.exception)
                            {
                                numDetCommit++;
                                det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                                if (config.grainImplementationType == ImplementationType.SNAPPER)
                                {
                                    det_emitTime.Add((task.Result.emitTime - task.Result.arriveTime).TotalMilliseconds);
                                    det_waitBatchScheduleTime.Add((task.Result.batchTime - task.Result.arriveTime).TotalMilliseconds);
                                    det_waitBatchCommitTime.Add((task.Result.commitTime - task.Result.finishTime).TotalMilliseconds);
                                    det_networkTime.Add((task.Result.arriveTime - reqs[task] + asyncReqEndTime - task.Result.commitTime).TotalMilliseconds);
                                    double exeTime;
                                    if (task.Result.emitTime > task.Result.batchTime) exeTime = (task.Result.finishTime - task.Result.emitTime).TotalMilliseconds;
                                    else exeTime = (task.Result.finishTime - task.Result.batchTime).TotalMilliseconds;
                                    det_executeTime.Add(exeTime);
                                }
                            }
                        }
                        else    // for non-det 
                        {
                            numNonDetTransaction++;
                            if (!task.Result.exception)
                            {
                                numNonDetCommit++;
                                latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                                emitTime.Add((task.Result.emitTime - task.Result.arriveTime).TotalMilliseconds);
                                executeTime.Add((task.Result.finishTime - task.Result.emitTime).TotalMilliseconds);
                                waitBatchCommitTime.Add((task.Result.commitTime - task.Result.finishTime).TotalMilliseconds);
                                networkTime.Add((task.Result.arriveTime - reqs[task] + asyncReqEndTime - task.Result.commitTime).TotalMilliseconds);
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

                // for Snapper
                Console.WriteLine($"thread {threadIndex} finishes epoch {eIndex}, total_num_det = {numDetTransaction}, det-commit = {numDetCommit}, total_num_nonDet = {numNonDetTransaction}, non-det-commit = {numNonDetCommit}, total_time = {endTime - startTime}, tp = {1000 * (numDetCommit + numNonDetCommit) / (endTime - startTime)}. ");
                Console.WriteLine($"NotSerilizable = {numNotSerializable}");
                var res = new WorkloadResults(numDetTransaction, numNonDetTransaction, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numDeadlock);
                res.setTime(latencies, networkTime, emitTime, executeTime, waitBatchCommitTime);
                res.setDetTime(det_latencies, det_networkTime, det_emitTime, det_waitBatchScheduleTime, det_executeTime, det_waitBatchCommitTime);
                results[threadIndex] = res;
                //Signal the completion of epoch
                threadAcks[eIndex].Signal();

                // for OrleansTxn
                /*
                Console.WriteLine($"thread {threadIndex} finishes epoch {eIndex}, total_num_txn = {numEmit}, commit = {numDetCommit}, total_time = {endTime - startTime}, tp = {1000 * numDetCommit / (endTime - startTime)}. ");
                var res = new WorkloadResults(numEmit, numNonDetTransaction, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numDeadlock);
                res.setTime(latencies, networkTime, emitTime, executeTime, waitBatchCommitTime);
                res.setDetTime(det_latencies, det_networkTime, det_emitTime, det_executeTime, det_waitBatchCommitTime);
                results[threadIndex] = res;
                //Signal the completion of epoch
                threadAcks[eIndex].Signal();*/
            }
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
            var clientConfig = new ClientConfiguration();
            for (int i = 0; i < config.numConnToClusterPerWorkerNode; i++)
            {
                if (Constants.localCluster) clients[i] = await clientConfig.StartClientWithRetries();
                else clients[i] = await clientConfig.StartClientWithRetriesToCluster();
            }
        }
        private static async void Initialize()
        {
            benchmarks = new SmallBankBenchmark[config.numThreadsPerWorkerNode];
            results = new WorkloadResults[config.numThreadsPerWorkerNode];

            for (int i = 0; i < config.numThreadsPerWorkerNode; i++)
            {
                switch (config.benchmark)
                {
                    case BenchmarkType.SMALLBANK:
                        benchmarks[i] = new SmallBankBenchmark();
                        benchmarks[i].generateBenchmark(config, skew, hotGrainRatio);
                        break;
                    default:
                        throw new Exception("Unknown benchmark type");
                }
            }

            await InitializeClients();
            InitializeThreads();
            initializationDone = true;
        }

        static void ProcessWork()
        {
            Console.WriteLine("====== WORKER ======");
            using (var controller = new SubscriberSocket(controllerAddress))
            {
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
                while (!initializationDone)
                    Thread.Sleep(100);

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
                    Console.WriteLine($"Received signal from controller. Running epoch {i} across {config.numThreadsPerWorkerNode} worker threads");
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
                foreach (var thread in threads)
                {
                    thread.Join();
                }
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
            var aggNetworkTime = new List<double>();
            var aggEmitTime = new List<double>();
            var aggExecuteTime = new List<double>();
            var aggWaitBatchCommitTime = new List<double>();
            var aggDetLatencies = new List<double>();
            var aggDetNetworkTime = new List<double>();
            var aggDetEmitTime = new List<double>();
            var aggDetWaitBatchScheduleTime = new List<double>();
            var aggDetExecuteTime = new List<double>();
            var aggDetWaitBatchCommitTime = new List<double>();
            aggLatencies.AddRange(results[0].latencies);
            aggNetworkTime.AddRange(results[0].networkTime);
            aggEmitTime.AddRange(results[0].emitTime);
            aggExecuteTime.AddRange(results[0].executeTime);
            aggWaitBatchCommitTime.AddRange(results[0].waitBatchCommitTime);
            aggDetLatencies.AddRange(results[0].det_latencies);
            aggDetNetworkTime.AddRange(results[0].det_networkTime);
            aggDetEmitTime.AddRange(results[0].det_emitTime);
            aggDetWaitBatchScheduleTime.AddRange(results[0].det_waitBatchScheduleTime);
            aggDetExecuteTime.AddRange(results[0].det_executeTime);
            aggDetWaitBatchCommitTime.AddRange(results[0].det_waitBatchCommitTime);
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
                aggNetworkTime.AddRange(results[i].networkTime);
                aggEmitTime.AddRange(results[i].emitTime);
                aggExecuteTime.AddRange(results[i].executeTime);
                aggWaitBatchCommitTime.AddRange(results[i].waitBatchCommitTime);
                aggDetLatencies.AddRange(results[i].det_latencies);
                aggDetNetworkTime.AddRange(results[i].det_networkTime);
                aggDetEmitTime.AddRange(results[i].det_emitTime);
                aggDetWaitBatchScheduleTime.AddRange(results[i].det_waitBatchScheduleTime);
                aggDetExecuteTime.AddRange(results[i].det_executeTime);
                aggDetWaitBatchCommitTime.AddRange(results[i].det_waitBatchCommitTime);
            }
            var res = new WorkloadResults(aggNumDetTransactions, aggNumNonDetTransactions, aggNumDetCommitted, aggNumNonDetCommitted, aggStartTime, aggEndTime, aggNumNotSerializable, aggNumDeadlock);
            res.setTime(aggLatencies, aggNetworkTime, aggEmitTime, aggExecuteTime, aggWaitBatchCommitTime);
            res.setDetTime(aggDetLatencies, aggDetNetworkTime, aggDetEmitTime, aggDetWaitBatchScheduleTime, aggDetExecuteTime, aggDetWaitBatchCommitTime);
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

            pipeSize = int.Parse(args[0]);
            skew = double.Parse(args[1]);
            hotGrainRatio = double.Parse(args[2]);
            Console.WriteLine($"pipeSize per thread = {pipeSize}, skew = {skew}, hotGrainRatio = {hotGrainRatio}");

            ProcessWork();
            //Console.ReadLine();
        }
    }
}