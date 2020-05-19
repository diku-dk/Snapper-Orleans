using System;
using System.Threading;
using System.Diagnostics;
using System.Configuration;
using System.Collections.Specialized;
using NetMQ.Sockets;
using NetMQ;
using Utilities;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace ExperimentProcess
{
    class Program
    {
        static Boolean LocalCluster;
        static IClusterClient[] clients;
        static String sinkAddress = ">tcp://localhost:5558";
        static String controllerAddress = ">tcp://localhost:5575";
        //static String sinkAddress = ">tcp://18.218.182.227:5558";         // controller public IP
        //static String controllerAddress = ">tcp://18.218.182.227:5575";   // controller public IP
        static PushSocket sink = new PushSocket(sinkAddress);
        static WorkloadResults[] results;        
        static IBenchmark[] benchmarks;
        static WorkloadConfiguration config;
        static Barrier[] barriers;
        static CountdownEvent[] threadAcks;
        static bool initializationDone = false;
        static Thread[] threads;

        static int global_tid = 0;

        private static async void ThreadWorkAsync(Object obj)
        {
            int threadIndex = (int)obj;
            var globalWatch = new Stopwatch();
            var benchmark = benchmarks[threadIndex];
            IClusterClient client = clients[threadIndex % config.numConnToClusterPerWorkerNode];
            Console.WriteLine("get into ThreadWorkAsync()");
            for(int eIndex = 0; eIndex < config.numEpochs; eIndex++)
            {
                int numCommit = 0;
                int numTransaction = 0;
                int numNonDetTxn = 0;
                var latencies = new List<double>();
                var abortType = new int[5];
                for (int i = 0; i < 5; i++) abortType[i] = 0;
                var tasks = new List<Task<FunctionResult>>();
                var reqs = new Dictionary<Task<FunctionResult>, TimeSpan>();
                //var tasks = new List<Task<TransactionContext>>();
                //var reqs = new Dictionary<Task<TransactionContext>, TimeSpan>();
                //Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                globalWatch.Restart();
                var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                do
                {
                    while(tasks.Count < config.asyncMsgLengthPerThread)
                    {
                        //Pipeline remaining tasks
                        var asyncReqStartTime = globalWatch.Elapsed;
                        var newTask = benchmark.newTransaction(client, global_tid);
                        //global_tid += numWorker;
                        reqs.Add(newTask, asyncReqStartTime);
                        tasks.Add(newTask);                      
                    } 
                    var task = await Task.WhenAny(tasks);
                    numTransaction++; //Count transactions now
                    var asyncReqEndTime = globalWatch.Elapsed;
                    bool noException = true;
                    try
                    {
                        //Needed to catch exception of individual task (not caught by Snapper's exception) which would not be thrown by WhenAny
                        await task;
                    } 
                    catch (Exception)    // this exception is only related to OrleansTransaction
                    {
                        noException = false;
                    }
                    if (!task.Result.isDet) numNonDetTxn++;
                    if (noException)
                    {
                        if (!task.Result.hasException())
                        {
                            numCommit++;
                            var latency = asyncReqEndTime - reqs[task];
                            latencies.Add(latency.TotalMilliseconds);
                        }
                        else
                        {
                            abortType[0] += task.Result.Exp_RWConflict ? 1 : 0;
                            abortType[1] += task.Result.Exp_NotSerializable ? 1 : 0;
                            abortType[2] += task.Result.Exp_AppLogic ? 1 : 0;
                            abortType[3] += task.Result.Exp_2PC ? 1 : 0;
                            abortType[4] += task.Result.Exp_UnExpect ? 1 : 0;
                        }
                    }
                    tasks.Remove(task);
                    reqs.Remove(task);
                } 
                while (globalWatch.ElapsedMilliseconds < config.epochDurationMSecs);                
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                globalWatch.Stop();
                Console.WriteLine($"Finish epoch {eIndex}, total_num_txn = {numTransaction}, non-det = {numNonDetTxn}, commit = {numCommit}, total_time = {endTime - startTime}, tp = {1000 * numCommit / (endTime - startTime)}. ");
                Console.WriteLine($"RWConflict = {abortType[0]}, NotSerilizable = {abortType[1]}, AppLogic = {abortType[2]}, 2PC = {abortType[3]}, UnExpect = {abortType[4]}. ");
                //Wait for the tasks exceeding epoch time but do not count them
                if (tasks.Count != 0)
                {
                    try
                    {
                        await Task.WhenAll(tasks);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Exception: {e.Message}. ");
                    }
                }
                WorkloadResults res = new WorkloadResults(numTransaction, numCommit, numNonDetTxn, startTime, endTime, latencies, abortType);
                results[threadIndex] = res;
                //Signal the completion of epoch
                threadAcks[eIndex].Signal();
            }
        }

        private static void InitializeThreads() {
            barriers = new Barrier[config.numEpochs];
            threadAcks = new CountdownEvent[config.numEpochs];
            for(int i=0; i<config.numEpochs; i++) {
                barriers[i] = new Barrier(config.numThreadsPerWorkerNode+1);
                threadAcks[i] = new CountdownEvent(config.numThreadsPerWorkerNode);
            }                
            
            //Spawn Threads        
            threads = new Thread[config.numThreadsPerWorkerNode];
            for(int i=0; i< config.numThreadsPerWorkerNode;i++) {
                int threadIndex = i;
                Thread thread = new Thread(ThreadWorkAsync);
                threads[threadIndex] = thread;
                thread.Start(threadIndex);                        
            }            
        }

        private static async Task InitializeClients() {
            clients = new IClusterClient[config.numConnToClusterPerWorkerNode];
            ClientConfiguration clientConfig = new ClientConfiguration();
            for(int i=0;i<config.numConnToClusterPerWorkerNode;i++) 
            {
                if (LocalCluster) clients[i] = await clientConfig.StartClientWithRetries();
                else clients[i] = await clientConfig.StartClientWithRetriesToCluster();
            }
        }
        private static async void Initialize() {
            benchmarks = new SmallBankBenchmark[config.numThreadsPerWorkerNode];
            results = new WorkloadResults[config.numThreadsPerWorkerNode];

            for(int i=0;i<config.numThreadsPerWorkerNode;i++) {
                switch(config.benchmark) {
                    case BenchmarkType.SMALLBANK:
                        benchmarks[i] = new SmallBankBenchmark();
                        benchmarks[i].generateBenchmark(config);
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
            // changed by Yijian
            // using (var controller = new PullSocket(controllerAddress))
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
                while(!initializationDone) 
                    Thread.Sleep(100);

                Console.WriteLine("Finished initialization, sending ACK to controller");
                //Send an ACK
                msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT_ACK);
                sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));

                for(int i=0;i<config.numEpochs;i++) {
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
                    var result = AggregateAcrossThreadsForEpoch();
                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH_ACK);
                    msg.contents = Helper.serializeToByteArray<WorkloadResults>(result);
                    sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                }

                Console.WriteLine("Finished running epochs, exiting");
                foreach (var thread in threads) {
                    thread.Join();
                }
            }
        }

        private static WorkloadResults AggregateAcrossThreadsForEpoch() 
        {
            Trace.Assert(results.Length >= 1);
            int aggNumCommitted = results[0].numCommitted;
            int aggNumTransactions = results[0].numTransactions;
            int aggNumNonDetTxn = results[0].numNonDetTxn;
            long aggStartTime = results[0].startTime;
            long aggEndTime = results[0].endTime;
            var aggLatencies = new List<double>();
            var aggAbortType = new int[5];
            for (int j = 0; j < 5; j++) aggAbortType[j] = results[0].abortType[j];
            aggLatencies.AddRange(results[0].latencies);
            for(int i = 1;i < results.Length; i++)    // reach thread has a result
            {
                aggNumCommitted += results[i].numCommitted;
                aggNumTransactions += results[i].numTransactions;
                aggNumNonDetTxn += results[i].numNonDetTxn;
                aggStartTime = (results[i].startTime < aggStartTime) ? results[i].startTime : aggStartTime;
                aggEndTime = (results[i].endTime < aggEndTime) ? results[i].endTime : aggEndTime;     // ????
                aggLatencies.AddRange(results[i].latencies);
                for (int j = 0; j < 5; j++) aggAbortType[j] += results[i].abortType[j];
            }
            return new WorkloadResults(aggNumTransactions, aggNumCommitted, aggNumNonDetTxn, aggStartTime, aggEndTime, aggLatencies, aggAbortType);
        }

        private static void InitializeValuesFromConfigFile()
        {
            var benchmarkFrameWorkSection = ConfigurationManager.GetSection("BenchmarkFrameworkConfig") as NameValueCollection;
            LocalCluster = bool.Parse(benchmarkFrameWorkSection["LocalCluster"]);
        }
        static void Main(string[] args)
        {
            Console.WriteLine("Worker is Started...");
            InitializeValuesFromConfigFile();
            ProcessWork();
        }
    }
}
