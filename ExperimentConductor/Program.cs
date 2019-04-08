using System;
using NetMQ.Sockets;
using NetMQ;
using System.Diagnostics;
using Utilities;

namespace ExperimentConductor
{
    class Program
    {
        static void PushToWorkersAndSink(WorkloadConfiguration workload) {
            // Task Ventilator
            // Binds PUSH socket to tcp://localhost:5557
            // Sends batch of tasks to workers via that socket
            Console.WriteLine("====== VENTILATOR ======");

            using (var workers = new PushSocket("@tcp://*:5557"))
            using (var sink = new PullSocket(">tcp://localhost:5558"))
            {
                Console.WriteLine("Press enter when worker are ready");
                Console.ReadLine();

                //the first message it "0" and signals start of batch
                //see the Sink.csproj Program.cs file for where this is used
                Console.WriteLine("Sending start of batch to Sink");
                var netMessage = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_CONFIG);
                netMessage.contents = Helper.serializeToByteArray<WorkloadConfiguration>(workload);
                sink.SendFrame(Helper.serializeToByteArray<NetworkMessageWrapper>(netMessage));

                Console.WriteLine("Sending tasks to workers");

                //initialise random number generator
                Random rand = new Random(0);

                //expected costs in Ms
                int totalMs = 0;

                
                workers.SendFrame(workload.ToString());
                
                Console.WriteLine("Total expected cost : {0} msec", totalMs);
                Console.WriteLine("Press Enter to quit");
                Console.ReadLine();
            }
        }

        static void PullFromWorkers()
        {
            {
                // Task Sink
                // Bindd PULL socket to tcp://localhost:5558
                // Collects results from workers via that socket
                Console.WriteLine("====== SINK ======");

                //socket to receive messages on
                using (var sink = new PullSocket("@tcp://localhost:5558"))
                {
                    //wait for start of batch (see Ventilator.csproj Program.cs)
                    var startOfBatchTrigger = sink.ReceiveFrameString();
                    Console.WriteLine("Seen start of batch");

                    //Start our clock now
                    var watch = Stopwatch.StartNew();

                    for (int taskNumber = 0; taskNumber < 100; taskNumber++)
                {
                        var workerDoneTrigger = sink.ReceiveFrameString();
                        if (taskNumber % 10 == 0)
                        {
                            Console.Write(":");
                        }
                        else
                        {
                            Console.Write(".");
                        }
                    }
                    watch.Stop();
                    //Calculate and report duration of batch
                    Console.WriteLine();
                    Console.WriteLine("Total elapsed time {0} msec", watch.ElapsedMilliseconds);
                    Console.ReadLine();
                }
            }
        }

        static void Main(string[] args)
        {
            //Nothing much
            var workLoad = new WorkloadConfiguration();
            PushToWorkersAndSink(workLoad);
            PullFromWorkers();
        }
    }
}
