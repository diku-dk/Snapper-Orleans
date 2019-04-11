using System;
using NetMQ.Sockets;
using NetMQ;
using System.Diagnostics;
using Utilities;
using System.Threading;

namespace ExperimentConductor
{
    class Program
    {
        static String workerAddress = "@tcp://localhost:5575";
        static String sinkAddress = ">tcp://localhost:5558";
        static int numOfWorkers = 1; 
        static int numOfThreadsPerWorker = 2;

        static void PushToWorkers(Object obj) {
            // Task Ventilator
            // Binds PUSH socket to tcp://localhost:5557
            // Sends batch of tasks to workers via that socket
            Console.WriteLine("====== VENTILATOR ======");

            WorkloadConfiguration workload = (WorkloadConfiguration)obj;
            using (var workers = new PushSocket(workerAddress))
            {
                Console.WriteLine("Press enter when worker are ready");
                Console.ReadLine();

                //the first message it "0" and signals start of batch
                //see the Sink.csproj Program.cs file for where this is used
                
                var netMessage = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_CONFIG);
                netMessage.contents = Helper.serializeToByteArray<WorkloadConfiguration>(workload);
                workers.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(netMessage));
                Console.WriteLine("Sending workload configuration to workers");
            }
        }

        static void PullFromWorkers()
        {
            {
                // Task Sink
                // Bindd PULL socket to tcp://localhost:5558
                // Collects results from workers via that socket
                Console.WriteLine("====== SINK ======");

                WorkloadResults[] results = new WorkloadResults[numOfWorkers * numOfThreadsPerWorker];
                //socket to receive results on
                using (var sink = new PullSocket(sinkAddress))
                {
                    for (int taskNumber = 0; taskNumber < numOfWorkers * numOfThreadsPerWorker; taskNumber++)
                    {
                        var resultMsg = Helper.deserializeFromByteArray<NetworkMessageWrapper>(sink.ReceiveFrameBytes());
                        //Parse the workloadResult
                        Debug.Assert(resultMsg.msgType == Utilities.MsgType.WORKLOAD_RESULTS);
                        results[taskNumber] = Helper.deserializeFromByteArray<WorkloadResults>(resultMsg.contents);
                    }
                    //Calculate and report the results
                }
                Console.WriteLine("====== SINK ======");
            }
        }


        static void Main(string[] args)
        {
            //Nothing much
            if(args.Length > 0)
            {
                workerAddress = args[0];
                sinkAddress = args[1];
            }
            var workLoad = new WorkloadConfiguration();
            workLoad.numWorkerNodes = numOfWorkers;
            workLoad.numThreadsPerWorkerNodes = numOfThreadsPerWorker;
            workLoad.totalTransactions = numOfWorkers * numOfThreadsPerWorker;

            Thread conducterThread = new Thread(PushToWorkers);
            conducterThread.Start(workLoad);

            Thread sinkThread = new Thread(PullFromWorkers);
            conducterThread.Start();

            conducterThread.Join();
            sinkThread.Join();
        }
    }
}
