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
    

        private static async void ThreadWorkAsync(Object obj)
        {
            ThreadWorkload workload = ((Tuple<ThreadWorkload, int>) obj).Item1;
            int threadIndex = ((Tuple<ThreadWorkload, int>)obj).Item2;
            ClientConfiguration config = new ClientConfiguration();
            IClusterClient client = null;
            if (LocalCluster)
                client = await config.StartClientWithRetries();
            else
                client = await config.StartClientWithRetriesToCluster();

            Stopwatch localWatch = new Stopwatch();
            localWatch.Start();
            Stopwatch globalWatch = new Stopwatch();
            int numCommit = 0;
            
            ITransactionExecutionGrain[] startGrains = new ITransactionExecutionGrain[workload.transactions.Count];
            long[] latencies = new long[workload.transactions.Count];
            int index = 0;
            foreach (var transaction in workload.transactions)
            {
                String grainName = transaction.Item1;
                var startGrainId = transaction.Item2;
                startGrains[index++] = client.GetGrain<ITransactionExecutionGrain>(startGrainId, grainName);
            }

            index = 0;
            globalWatch.Start();
            foreach (var transaction in workload.transactions)
            {
                String grainName = transaction.Item1;
                var startGrainId = transaction.Item2;
                var functionName = transaction.Item3;
                var functionInputs = transaction.Item4;
                var isDeterministic = transaction.Item5;
                var accessInformation = transaction.Item6;

                Task<FunctionResult> task;
                localWatch.Restart();
                if (isDeterministic)
                {
                    task = startGrains[index].StartTransaction(accessInformation, functionName, functionInputs);
                }
                else
                {
                    task = startGrains[index].StartTransaction(functionName, functionInputs);
                }
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
                index++;
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

            WorkloadResults res = new WorkloadResults(workload.transactions.Count, numCommit, min, max, average, throughput);
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
                    var workload = Helper.deserializeFromByteArray<WorkloadConfiguration>(workloadMsg.contents);
                    var numOfThreads = workload.numThreadsPerWorkerNodes;
                    clients = new IClusterClient[numOfThreads];
                    //Spawn Threads
                    Thread[] threads = new Thread[numOfThreads];
                    results = new WorkloadResults[numOfThreads];
                    for(int i=0; i< numOfThreads; i++)
                    {
                        int threadIndex = i;
                        Thread thread = new Thread(ThreadWorkAsync);
                        threads[threadIndex] = thread;
                        thread.Start(new Tuple<ThreadWorkload, int>(new ThreadWorkload(workload), threadIndex));                        
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


        public class ThreadWorkload
        {
            //Tuple<GrainName, Grain ID, Function Name, Function Inputs, isDeterministic, Access Information>
            public List<Tuple<String, Guid, String, FunctionInput, Boolean, Dictionary<Guid, Tuple<String, int>>>> transactions;
          
            public ThreadWorkload(WorkloadConfiguration config)
            {
                transactions = new List<Tuple<string, Guid, string, FunctionInput, bool, Dictionary<Guid, Tuple<string, int>>>>();
                int numOfTransactions = config.totalTransactions / (config.numWorkerNodes * config.numThreadsPerWorkerNodes);

                for(int i=0; i< numOfTransactions; i++)
                {
                    var args = new TransferInput(1, 2, 10);
                    var grainName = "AccountTransfer.Grains.AccountGrain";
                    var startGrainId = Helper.convertUInt32ToGuid(1);
                    var functionName = "Transfer";
                    var functionInputs = new FunctionInput(args);
                    var isDeterministic = false;
                    var accessInformation = new Dictionary<Guid, Tuple<string, int>>();
                    transactions.Add(new Tuple<string, Guid, string, FunctionInput, bool, Dictionary<Guid, Tuple<string, int>>>
                        (grainName, startGrainId, functionName, functionInputs, isDeterministic, accessInformation));
                }

            }
            public ThreadWorkload()
            {
                transactions = new List<Tuple<string, Guid, string, FunctionInput, bool, Dictionary<Guid, Tuple<string, int>>>>();
            }
        }
        

        static void Main(string[] args)
        {
            Console.WriteLine("Worker is Started...");
            ProcessWork();
        }
    }
}
