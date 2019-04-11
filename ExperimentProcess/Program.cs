using System;
using System.Threading;
using System.Diagnostics;
using NetMQ.Sockets;
using NetMQ;
using Utilities;
using Orleans;
using OrleansClient;
using System.Threading.Tasks;

namespace ExperimentProcess
{
    class Program
    {
        static WorkloadResults res;
        static Boolean LocalCluster = true;
        static IClusterClient[] clients;

        private static async void ThreadWorkAsync(object obj)
        {
            int threadIdx = (int) obj;
            ClientConfiguration config = new ClientConfiguration();
            IClusterClient client = null;
            if (LocalCluster)
                client = await config.StartClientWithRetries();
            else
                client = await config.StartClientWithRetriesToCluster();

            await DoClientWork(client);
        }

        static async Task DoClientWork(IClusterClient client)
        {
            var Test = new GlobalCoordinatorTest(client);
            await Test.ConcurrentDetTransaction();
        }

        static void ProcessWork()
        {
            // Task Worker
            // Connects PULL socket to tcp://localhost:5557
            // collects workload for socket from Ventilator via that socket
            // Connects PUSH socket to tcp://localhost:5558
            // Sends results to Sink via that socket
            Console.WriteLine("====== WORKER ======");

            using (var receiver = new PullSocket(">tcp://localhost:5575"))
            using (var sink = new PushSocket(">tcp://localhost:5558"))
            {
                //process tasks forever
                while (true)
                {
                    //workload from the vetilator is a simple delay
                    //to simulate some work being done, see
                    //Ventilator.csproj Proram.cs for the workload sent
                    //In real life some more meaningful work would be done
                    var workloadMsg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(receiver.ReceiveFrameBytes());
                    //Parse the workloadMsg
                    Debug.Assert(workloadMsg.msgType == Utilities.MsgType.WORKLOAD_CONFIG);
                    var workload = Helper.deserializeFromByteArray<WorkloadConfiguration>(workloadMsg.contents);
                    clients = new IClusterClient[workload.numThreadsPerWorkerNodes];
                    //Spawn Threads
                    Thread[] threads = new Thread[workload.numThreadsPerWorkerNodes];
                    for(int i=0; i<workload.numThreadsPerWorkerNodes;i++)
                    {
                        //var thread = new Thread(() => ThreadWorkAsync(i));
                        Thread thread = new Thread(ThreadWorkAsync);
                        threads[i] = thread;
                        thread.Start(i);                        
                    }

                    foreach (var thread in threads)
                    {
                        thread.Join();
                    }

                    var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_RESULTS);
                    msg.contents = Helper.serializeToByteArray<WorkloadResults>(res);
                    //sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
                }
            }
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            ProcessWork();
        }
    }
}
