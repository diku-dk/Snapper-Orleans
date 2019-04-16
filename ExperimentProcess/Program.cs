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

namespace ExperimentProcess
{
    class Program
    {
        static WorkloadResults res;
        static Boolean LocalCluster = true;
        static IClusterClient[] clients;
        static String sinkAddress = ">tcp://localhost:5558";
        static String conductorAddress = ">tcp://localhost:5575";


        private static async void ThreadWorkAsync(Object obj)
        {
            ThreadWorkload workload = (ThreadWorkload) obj;
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
                if (task.Result.hasException() == true)
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
            var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_RESULTS);
            msg.contents = Helper.serializeToByteArray<WorkloadResults>(res);
            using (var sink = new PushSocket(sinkAddress))
            {
                sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
            }
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
                    for(int i=0; i<workload.numThreadsPerWorkerNodes;i++)
                    {
                        Thread thread = new Thread(ThreadWorkAsync);
                        threads[i] = thread;
                        thread.Start(new ThreadWorkload());                        
                    }
                    foreach (var thread in threads)
                    {
                        thread.Join();
                    }
                }
            }
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
                    ;
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
