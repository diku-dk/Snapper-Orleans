using System;
using System.Threading;
using System.Diagnostics;
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
        static String conductorAddress = ">tcp://localhost:5575";
        static PushSocket sink = new PushSocket(sinkAddress);
        static List<WorkloadResults>[] results;
        static volatile int finished = 0;
        static IBenchmark benchmark;
        static WorkloadConfiguration config;
        static Barrier[] barriers;

        private static async void ThreadWorkAsync(Object obj)
        {

            int threadIndex = (int)obj;
            ClientConfiguration clientConfig = new ClientConfiguration();
            IClusterClient client = null;
            if (LocalCluster)
                client = await clientConfig.StartClientWithRetries();
            else
                client = await clientConfig.StartClientWithRetriesToCluster();
            results[threadIndex] = new List<WorkloadResults>();
            Stopwatch localWatch = new Stopwatch();
            Stopwatch globalWatch = new Stopwatch();
            int numCommit = 0;
            int numTransaction = 0;
            var latencies = new List<long>();

            for(int eIndex = 0; eIndex < config.numEpoch; eIndex++)
            {
                globalWatch.Restart();
                long startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                while (globalWatch.ElapsedMilliseconds < config.epochInMiliseconds)
                {
                    localWatch.Restart();
                    Task<FunctionResult> task = benchmark.newTransaction(client);
                    await task;
                    numTransaction++;
                    localWatch.Stop();
                    if (task.Result.hasException() != true)
                    {
                        numCommit++;
                        var latency = localWatch.ElapsedMilliseconds;
                        latencies.Add(latency);
                    }
                }
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                globalWatch.Stop();
                WorkloadResults res = new WorkloadResults(numTransaction, numCommit, startTime, endTime, latencies);
                results[threadIndex].Add(res);
                barriers[eIndex].SignalAndWait();
                Console.WriteLine($"Completed epoch {eIndex} at {DateTime.Now.ToString()}");
            }

            Interlocked.Increment(ref finished);
            //Console.WriteLine($"{res.numSuccessFulTxns} of {res.numTxns} transactions are committed. Latency: {res.averageLatency}. Throughput: {res.throughput}.\n");
        }



        static void ProcessWork()
        {
            Console.WriteLine("====== WORKER ======");
            using (var conductor = new PullSocket(conductorAddress))
            {
                while (true)
                {
                    var workloadMsg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(conductor.ReceiveFrameBytes());
                    //Parse the workloadMsg
                    Debug.Assert(workloadMsg.msgType == Utilities.MsgType.WORKLOAD_CONFIG);
                    config = Helper.deserializeFromByteArray<WorkloadConfiguration>(workloadMsg.contents);
                    switch (config.benchmark)
                    {
                        case BenchmarkType.SMALLBANK:
                            benchmark = new SmallBankBenchmark();
                            break;
                        default:
                            benchmark = new SmallBankBenchmark();
                            break;
                    }
                    benchmark.generateBenchmark(config);

                    var numOfThreads = config.numThreadsPerWorkerNodes;
                    var numOfEpochs = config.numEpoch;

                    barriers = new Barrier[numOfEpochs];
                    for(int i=0; i<barriers.Length; i++)
                        barriers[i] = new Barrier(participantCount: numOfThreads);
                    clients = new IClusterClient[numOfThreads];
                    //Spawn Threads
                    Thread[] threads = new Thread[numOfThreads];
                    results = new List<WorkloadResults>[numOfThreads];
                    for(int i=0; i< numOfThreads; i++)
                    {
                        int threadIndex = i;
                        Thread thread = new Thread(ThreadWorkAsync);
                        threads[threadIndex] = thread;
                        thread.Start(threadIndex);                        
                    }
                    foreach (var thread in threads)
                    {
                        thread.Join();
                    }

                    Thread aggregate = new Thread(AggregateAndSendToSink);
                    aggregate.Start(numOfThreads);
                    aggregate.Join();
                }
            }
        }



        public static void AggregateAndSendToSink(Object obj)
        {
            int expectedResults = (int)obj;
            while(finished < expectedResults)
            {
                Thread.Sleep(10000);
            }
            
            var result = new AggregatedWorkloadResults(results);
            var msg = new NetworkMessageWrapper(Utilities.MsgType.AGGREGATED_WORKLOAD_RESULTS);
            msg.contents = Helper.serializeToByteArray<AggregatedWorkloadResults>(result);
            sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
        }
        

        static void Main(string[] args)
        {
            Console.WriteLine("Worker is Started...");
            ProcessWork();
        }
    }
}
