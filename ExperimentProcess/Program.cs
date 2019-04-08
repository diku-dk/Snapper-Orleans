using System;
using System.Threading;
using System.Diagnostics;
using NetMQ.Sockets;
using NetMQ;
using Utilities;

namespace ExperimentProcess
{
    class Program
    {
        static WorkloadResults res;
        static void ThreadWork(int workerNumber)
        {
            res.startTime[workerNumber] = DateTime.Now.TimeOfDay.TotalMilliseconds;
            //Do the work
            res.numTxns[workerNumber]++;
            res.numSuccessFulTxns[workerNumber]++;
            res.endTime[workerNumber] = DateTime.Now.TimeOfDay.TotalMilliseconds;
        }
        static void ProcessWork()
        {
            // Task Worker
            // Connects PULL socket to tcp://localhost:5557
            // collects workload for socket from Ventilator via that socket
            // Connects PUSH socket to tcp://localhost:5558
            // Sends results to Sink via that socket
            Console.WriteLine("====== WORKER ======");

            using (var receiver = new PullSocket(">tcp://localhost:5557"))
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
                    //Spawn Threads
                    Thread[] threads = new Thread[workload.numThreadsPerWorkerNodes];
                    for(int i=0; i<workload.numThreadsPerWorkerNodes;i++)
                    {
                        var thread = new Thread(() => ThreadWork(i));
                        threads[i] = thread;
                        thread.Start();                        
                    }

                    foreach (var thread in threads)
                    {
                        thread.Join();
                    }

                    var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_RESULTS);
                    msg.contents = Helper.serializeToByteArray<WorkloadResults>(res);
                    sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(msg));
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
