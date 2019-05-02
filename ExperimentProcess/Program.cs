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
        static WorkloadResults[] results;
        static volatile int finished = 0;
        static IBenchmark benchmark;
        static WorkloadConfiguration config;

        private static async void ThreadWorkAsync(Object obj)
        {

            int threadIndex = (int)obj;
            ClientConfiguration clientConfig = new ClientConfiguration();
            IClusterClient client = null;
            if (LocalCluster)
                client = await clientConfig.StartClientWithRetries();
            else
                client = await clientConfig.StartClientWithRetriesToCluster();

            Stopwatch localWatch = new Stopwatch();
            
            Stopwatch globalWatch = new Stopwatch();
            int numCommit = 0;
            int numofTransactions = config.totalTransactions;
            long[] latencies = new long[numofTransactions];
            globalWatch.Start();
            for (int index = 0; index < numofTransactions; index++)
            {
                localWatch.Restart();
                Task<FunctionResult> task = benchmark.newTransaction(client);
                await task;
                localWatch.Stop();
                if (task.Result.hasException() != true)
                {
                    numCommit++;
                    var latency = localWatch.ElapsedMilliseconds;
                    latencies[index] = latency;
                }
                else
                    latencies[index] = -1;
            }
            globalWatch.Stop();
            
            var totalLatency = globalWatch.ElapsedMilliseconds;

            long min = long.MaxValue;
            long max = long.MinValue;
            long average = 0;
            foreach( var latency in latencies){
                if(latency != -1)
                {
                    min = min > latency ? latency : min;
                    max = max < latency ? latency : max;
                    average += latency;
                }
            }
            average /= numCommit;
            float throughput = (float)numCommit / ((float)totalLatency / (float)1000);

            WorkloadResults res = new WorkloadResults(numofTransactions, numCommit, min, max, average, throughput);
            results[threadIndex]= res;
            Interlocked.Increment(ref finished);
            Console.WriteLine($"{res.numSuccessFulTxns} of {res.numTxns} transactions are committed. Latency: {res.averageLatency}. Throughput: {res.throughput}.\n");
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
                    clients = new IClusterClient[numOfThreads];
                    //Spawn Threads
                    Thread[] threads = new Thread[numOfThreads];
                    results = new WorkloadResults[numOfThreads];
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
            long min = long.MaxValue;
            long max = long.MinValue;
            long average = 0;
            int committed = 0;
            int totalTxns = 0;
            float throughput = 0;
            foreach (var res in results)
            {
                min = min < res.minLatency ? min : res.minLatency;
                max = max > res.maxLatency ? max : res.maxLatency;
                committed += res.numSuccessFulTxns;
                totalTxns += res.numTxns;
                average += res.averageLatency;
                throughput += res.throughput;
            }
            average /= results.Length;
            throughput /= results.Length;

            var result = new WorkloadResults(totalTxns, committed, min, max, average, throughput);
            var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_RESULTS);
            msg.contents = Helper.serializeToByteArray<WorkloadResults>(result);
            sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
        }
        

        static void Main(string[] args)
        {
            Console.WriteLine("Worker is Started...");
            ProcessWork();
        }
    }
}
