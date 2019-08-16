using System;
using System.Threading;
using System.Diagnostics;
using System.Configuration;
using System.Collections.Specialized;
using NetMQ.Sockets;
using NetMQ;
using Utilities;
using Orleans;
using OrleansClient;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface;
using AccountTransfer.Interfaces;
using AccountTransfer.Grains;

namespace ExperimentProcess
{
    class Program
    {
        static Boolean LocalCluster = true;
        static IClusterClient[] clients;
        static String sinkAddress = "@tcp://localhost:5558";
        static String controllerAddress = ">tcp://localhost:5575";
        static PushSocket sink = new PushSocket(sinkAddress);
        static WorkloadResults[] results;        
        static IBenchmark[] benchmarks;
        static WorkloadConfiguration config;
        static Barrier[] barriers;
        static CountdownEvent[] threadAcks;
        static bool initializationDone = false;
        static Thread[] threads;

        private static async void ThreadWorkAsync(Object obj)
        {

            int threadIndex = (int)obj;
            var globalWatch = new Stopwatch();
            var benchmark = benchmarks[threadIndex];
            IClusterClient client = clients[threadIndex % config.numConnToClusterPerWorkerNode];

            for(int eIndex = 0; eIndex < config.numEpochs; eIndex++)
            {
                int numCommit = 0;
                int numTransaction = 0;
                var latencies = new List<double>();
                //Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                globalWatch.Restart();
                var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                var tasks = new List<Task<FunctionResult>>();
                var reqs = new Dictionary<Task<FunctionResult>, TimeSpan>();
                do
                {
                    while(tasks.Count < config.asyncMsgLengthPerThread)
                    {
                        //Pipeline remaining tasks
                        var asyncReqStartTime = globalWatch.Elapsed;
                        var newTask = benchmark.newTransaction(client);
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
                    } catch (Exception)
                    {
                        noException = false;
                    }
                    
                    if (noException && task.Result.hasException() != true)
                    {
                        numCommit++;
                        var latency = asyncReqEndTime - reqs[task];
                        latencies.Add(latency.TotalMilliseconds);
                    }
                    tasks.Remove(task);
                    reqs.Remove(task);
                } while (globalWatch.ElapsedMilliseconds < config.epochDurationMSecs);                
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                globalWatch.Stop();
                //Wait for the tasks exceeding epoch time but do not count them
                if (tasks.Count != 0)
                {                    
                    await Task.WhenAll(tasks);
                }
                WorkloadResults res = new WorkloadResults(numTransaction, numCommit, startTime, endTime, latencies);
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

            for(int i=0;i<config.numConnToClusterPerWorkerNode;i++) {
                if (LocalCluster)
                    clients[i] = await clientConfig.StartClientWithRetries();
            else
                clients[i] = await clientConfig.StartClientWithRetriesToCluster();
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
            using (var controller = new PullSocket(controllerAddress))
            {
                //Acknowledge the controller thread
                var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKER_CONNECT);
                sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                Console.WriteLine("Connected to controller");

                //Wait to receive workload msg
                msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(controller.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT);
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
                    //Wait for EPOCH RUN signal
                    msg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(controller.ReceiveFrameBytes());
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

        private static WorkloadResults AggregateAcrossThreadsForEpoch() {
            Trace.Assert(results.Length >= 1);
            int aggNumCommitted = results[0].numCommitted;
            int aggNumTransactions = results[0].numTransactions;
            long aggStartTime = results[0].startTime;
            long aggEndTime = results[0].endTime;
            var aggLatencies = new List<double>();
            aggLatencies.AddRange(results[0].latencies);
            for(int i=1;i<results.Length;i++)
            {
                aggNumCommitted += results[i].numCommitted;
                aggNumTransactions += results[i].numTransactions;
                aggStartTime = (results[i].startTime < aggStartTime) ? results[i].startTime : aggStartTime;
                aggEndTime = (results[i].endTime < aggEndTime) ? results[i].endTime : aggEndTime;
                aggLatencies.AddRange(results[i].latencies);
            }
            return new WorkloadResults(aggNumTransactions, aggNumCommitted, aggStartTime, aggEndTime, aggLatencies);
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
