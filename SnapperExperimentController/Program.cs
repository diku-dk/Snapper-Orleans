using NetMQ;
using System;
using Utilities;
using NetMQ.Sockets;
using System.Threading;
using System.Diagnostics;
using SnapperExperimentProcess;
using MessagePack;
using System.Xml;
using System.Collections.Generic;

namespace SnapperExperimentController
{
    static class Program
    {
        // for communication between ExpController and ExpProcess
        static PullSocket inputSocket;
        static PublisherSocket outputSocket;
        static CountdownEvent ackedWorkers;

        static List<WorkloadConfiguration> workloadGroup;

        static ServerConnector serverConnector;
        static ExperimentResultAggregator resultAggregator;

        static void Main()
        {
            GenerateWorkLoadFromXMLFile();

            // initialize silo
            serverConnector = new ServerConnector();
            serverConnector.InitiateClientAndServer();
            serverConnector.LoadGrains();

            // build connection between the controller and workers
            ConnectWorkers(workloadGroup.Count);
            
            // start running experiments
            foreach (var workload in workloadGroup)
            {
                for (int i = 0; i < Constants.maxNumReRun; i++)
                {
                    Console.WriteLine($"Run experiment for {i}th time: grainSkewness = {workload.grainSkewness * 100.0}%, pactPercent = {workload.pactPercent}%, distPercent = {workload.distPercent}%");
                    resultAggregator = new ExperimentResultAggregator(workload);

                    //Start the controller thread
                    var outputThread = new Thread(PushToWorkers);
                    outputThread.Start(workload);

                    //Start the sink thread
                    var inputThread = new Thread(PullFromWorkers);
                    inputThread.Start();

                    //Wait for the threads to exit
                    inputThread.Join();
                    outputThread.Join();

                    var success = resultAggregator.AggregateResultsAndPrint();
                    if (success) break;
                }

                Console.WriteLine("Finished running experiment.");
                Thread.Sleep(5000);
            }

            // terminate workers
            TerminateWorkers();
        }

        static void GenerateWorkLoadFromXMLFile()
        {
            var path = Constants.dataPath + "config.xml";
            var xmlDoc = new XmlDocument();
            xmlDoc.Load(path);
            var rootNode = xmlDoc.DocumentElement;

            var txnSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("txnSize").FirstChild.Value.Split(","), x => int.Parse(x));
            
            var grainSkewnessGroup = Array.ConvertAll(rootNode.SelectSingleNode("grainSkewness").FirstChild.Value.Split(","), x => double.Parse(x) / 100.0);
            var actPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("actPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));
            var pactPipeSizeGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPipeSize").FirstChild.Value.Split(","), x => int.Parse(x));

            var pactPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("pactPercent").FirstChild.Value.Split(","), x => int.Parse(x));
            
            var distPercentGroup = Array.ConvertAll(rootNode.SelectSingleNode("distPercent").FirstChild.Value.Split(","), x => int.Parse(x));

            workloadGroup = new List<WorkloadConfiguration>();
            for (int i = 0; i < txnSizeGroup.Length; i++)
            {
                var txnSize = txnSizeGroup[i];
                for (int j = 0; j < grainSkewnessGroup.Length; j++)
                {
                    var grainSkewness = grainSkewnessGroup[j];
                    var actPipeSize = actPipeSizeGroup[j];
                    var pactPipeSize = pactPipeSizeGroup[j];
                    for (int k = 0; k < pactPercentGroup.Length; k++)
                    {
                        var pactPercent = pactPercentGroup[k];
                        for (int m = 0; m < distPercentGroup.Length; m++)
                        {
                            var distPercent = distPercentGroup[m];
                            var workload = new WorkloadConfiguration(txnSize, grainSkewness, actPipeSize, pactPipeSize, pactPercent, distPercent);
                            workloadGroup.Add(workload);
                        }
                    }
                }
            }
        }

        static void ConnectWorkers(int numExperiment)
        {
            if (Constants.RealScaleOut == false)
            {
                // controller and all workers are deployed on one machine
                inputSocket = new PullSocket(Constants.controller_InputAddress);
                outputSocket = new PublisherSocket(Constants.controller_OutputAddress);
            }
            else
            {
                // controller and workers are deployed on different machine
                inputSocket = new PullSocket($"@tcp://{Helper.GetLocalIPAddress()}:{Constants.controller_InputPort}");
                outputSocket = new PublisherSocket($"@tcp://*:{Constants.controller_OutputPort}");
            }

            ackedWorkers = new CountdownEvent(Constants.numWorker);
            Console.WriteLine("Wait for workers to connect...");
            NetworkMessage msg;
            for (int i = 0; i < Constants.numWorker; i++)
            {
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(inputSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == Utilities.MsgType.WORKER_CONNECT);
                Console.WriteLine($"Receive WORKER_CONNECT from worker {i}");
            }

            msg = new NetworkMessage(Utilities.MsgType.CONFIRM, MessagePackSerializer.Serialize(numExperiment));
            outputSocket.SendMoreFrame("CONFIRM").SendFrame(MessagePackSerializer.Serialize(msg));
            Console.WriteLine($"Publish confirmation to all workers.");
        }

        static void TerminateWorkers()
        {
            Console.WriteLine("Terminate workers...");
            var msg = new NetworkMessage(Utilities.MsgType.TERMINATE);
            outputSocket.SendMoreFrame("WORKLOAD_INIT").SendFrame(MessagePackSerializer.Serialize(msg));
        }

        static void WaitForWorkerAcksAndReset()
        {
            ackedWorkers.Wait();
            ackedWorkers.Reset(Constants.numWorker); //Reset for next ack, not thread-safe but provides visibility, ok for us to use due to lock-stepped (distributed producer/consumer) usage pattern i.e., Reset will never called concurrently with other functions (Signal/Wait)            
        }

        static void PushToWorkers(object obj)
        {
            Console.WriteLine($"Sent workload configuration to {Constants.numWorker} worker nodes");
            var workload = (WorkloadConfiguration)obj;
            var msg = new NetworkMessage(Utilities.MsgType.WORKLOAD_INIT, MessagePackSerializer.Serialize(workload));
            outputSocket.SendMoreFrame("WORKLOAD_INIT").SendFrame(MessagePackSerializer.Serialize(msg));

            Console.WriteLine($"Coordinator waits for WORKLOAD_INIT_ACK");
            WaitForWorkerAcksAndReset();
            Console.WriteLine($"Receive workload configuration ack from {Constants.numWorker} worker nodes");

            for (int i = 0; i < Constants.numEpoch; i++)
            {
                //Send the command to run an epoch
                Console.WriteLine($"Running Epoch {i} on {Constants.numWorker} worker nodes");
                msg = new NetworkMessage(Utilities.MsgType.RUN_EPOCH);
                outputSocket.SendMoreFrame("RUN_EPOCH").SendFrame(MessagePackSerializer.Serialize(msg));
                WaitForWorkerAcksAndReset();
                Console.WriteLine($"Finished running epoch {i} on {Constants.numWorker} worker nodes");

                serverConnector.ResetOrderGrain();
                Console.WriteLine($"11111111111111111111111");
                serverConnector.CheckGC();
                Console.WriteLine($"222222222222222222");
            }
        }

        static void PullFromWorkers()
        {
            for (int i = 0; i < Constants.numWorker; i++)
            {
                var msg = MessagePackSerializer.Deserialize<NetworkMessage>(inputSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT_ACK);
                Console.WriteLine($"Receive WORKLOAD_INIT_ACT from worker {i}");
                ackedWorkers.Signal();
            }

            //Wait for epoch acks
            for (int i = 0; i < Constants.numEpoch; i++)
            {
                for (int j = 0; j < Constants.numWorker; j++)
                {
                    var msg = MessagePackSerializer.Deserialize<NetworkMessage>(inputSocket.ReceiveFrameBytes());
                    Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH_ACK);
                    var result = MessagePackSerializer.Deserialize<WorkloadResult>(msg.content);
                    resultAggregator.SetResult(i, j, result);
                    ackedWorkers.Signal();
                }
            }
        }
    }
}