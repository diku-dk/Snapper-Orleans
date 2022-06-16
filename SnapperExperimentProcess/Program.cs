using NetMQ;
using System;
using Orleans;
using Utilities;
using NetMQ.Sockets;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using MessagePack;

namespace SnapperExperimentProcess
{
    static class Program
    {
        static int workerID = 0;
        static int maxNumExperiment = 0;
        // for communication between ExpController and ExpProcess
        static SubscriberSocket inputSocket;
        static PushSocket outputSocket;
        static CountdownEvent[] threadAcks;
        static WorkloadConfiguration workload;
        static bool initializationDone;

        static int numDetConsumer;
        static int numNonDetConsumer;
        static int detBufferSize;
        static int nonDetBufferSize;

        static Thread producerThread;
        static Thread[] concumerThreads;         // used to submit transaction requests
        static Barrier[] barriers;
        static IClusterClient[] clients;
        static IBenchmark[] benchmarks;
        static bool[] isEpochFinish;
        static bool[] isProducerFinish;
        static Dictionary<int, Queue<Tuple<bool, RequestData>>> shared_requests;                // <epoch, <isDet, grainIDs>>
        static Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>> thread_requests;  // <epoch, <consumerID, grainIDs>>

        static WorkloadGenerator workloadGenerator;
        static WorkloadResult[] results;

        static void Main(string[] args)
        {
            workerID = int.Parse(args[0]);
            if (Constants.LocalCluster == false && Constants.LocalTest == false)
            {
                if (Constants.RealScaleOut == false)
                {
                    Debug.Assert(Environment.ProcessorCount >= Constants.numWorker * Constants.numCPUPerSilo);
                    Helper.SetCPU(workerID, "SnapperExperimentProcess", Constants.numCPUPerSilo);
                }
                else
                {
                    Debug.Assert(Environment.ProcessorCount >= Constants.numCPUPerSilo);
                    Helper.SetCPU(0, "SnapperExperimentProcess", Constants.numCPUPerSilo);
                } 
            }

            ConnectController();

            for (int i = 0; i < maxNumExperiment; i++)
            {
                var isContinue = ProcessWork(i);
                if (isContinue == false) break;
            } 
        }

        static void ConnectController()
        {
            if (Constants.RealScaleOut == false)
            {
                // controller and all workers are deployed on one machine
                inputSocket = new SubscriberSocket(Constants.worker_InputAddress);
                outputSocket = new PushSocket(Constants.worker_OutputAddress);
            }
            else
            {
                // controller and workers are deployed on different machine
                inputSocket = new SubscriberSocket($">tcp://{Constants.controller_PublicIPAddress}:{Constants.controller_OutputPort}");
                outputSocket = new PushSocket($">tcp://{Constants.controller_PublicIPAddress}:{Constants.controller_InputPort}");
            }

            // worker ==> controller: WORKER_CONNECT
            // controller ==> worker: CONFIRM
            // worker ==> controller: CONFIRM
            inputSocket.Subscribe("CONFIRM");
            var msg = new NetworkMessage(Utilities.MsgType.WORKER_CONNECT);
            outputSocket.SendFrame(MessagePackSerializer.Serialize(msg));
            Console.WriteLine("Connected to controller...");

            inputSocket.Options.ReceiveHighWatermark = 1000;
            inputSocket.ReceiveFrameString();
            msg = MessagePackSerializer.Deserialize<NetworkMessage>(inputSocket.ReceiveFrameBytes());
            Trace.Assert(msg.msgType == Utilities.MsgType.CONFIRM);
            var numExperiment = MessagePackSerializer.Deserialize<int>(msg.content);
            maxNumExperiment = Constants.maxNumReRun * numExperiment;
            Console.WriteLine($"Receive numExperiment = {numExperiment} from the controller.");
        }

        static bool ProcessWork(int experimentID)
        {
            Console.WriteLine($"Run experiment {experimentID}...");

            inputSocket.Subscribe("WORKLOAD_INIT");
            inputSocket.Options.ReceiveHighWatermark = 1000;
            inputSocket.ReceiveFrameString();
            var msg = MessagePackSerializer.Deserialize<NetworkMessage>(inputSocket.ReceiveFrameBytes());
            
            if (msg.msgType == Utilities.MsgType.TERMINATE) return false;

            Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT);
            workload = MessagePackSerializer.Deserialize<WorkloadConfiguration>(msg.content);
            Console.WriteLine($"Receive workload configuration, grainSkewness = {workload.grainSkewness * 100.0}%, pactPercent = {workload.pactPercent}%, distPercent = {workload.distPercent}%.");

            inputSocket.Unsubscribe("WORKLOAD_INIT");
            inputSocket.Subscribe("RUN_EPOCH");

            // Initialize threads and other data-structures for epoch runs
            initializationDone = false;
            Initialize();
            while (!initializationDone) Thread.Sleep(100);

            Console.WriteLine("Finished initialization, sending ACK to controller.");
            msg = new NetworkMessage(Utilities.MsgType.WORKLOAD_INIT_ACK);
            outputSocket.SendFrame(MessagePackSerializer.Serialize(msg));

            for (int i = 0; i < Constants.numEpoch; i++)
            {
                inputSocket.ReceiveFrameString();
                msg = MessagePackSerializer.Deserialize<NetworkMessage>(inputSocket.ReceiveFrameBytes());
                Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH);
                Console.WriteLine($"Start running epoch {i}...");
                //Signal the barrier
                barriers[i].SignalAndWait();
                // Wait for all threads to finish the epoch
                threadAcks[i].Wait();
                var result = ExperimentResultAggregator.AggregateResultForEpoch(results);
                msg = new NetworkMessage(Utilities.MsgType.RUN_EPOCH_ACK, MessagePackSerializer.Serialize(result));
                outputSocket.SendFrame(MessagePackSerializer.Serialize(msg));
            }

            Console.WriteLine("Finished running epochs, exiting");
            foreach (var thread in concumerThreads) thread.Join();
            producerThread.Join();
            return true;
        }

        static async void ThreadWorkAsync(object obj)
        {
            var input = (Tuple<int, bool>)obj;
            int threadIndex = input.Item1;
            var isDet = input.Item2;
            var pipeSize = isDet ? workload.pactPipeSize : workload.actPipeSize;
            var globalWatch = new Stopwatch();
            var benchmark = benchmarks[threadIndex];
            var client = clients[threadIndex % (numDetConsumer + numNonDetConsumer)];
            Console.WriteLine($"thread = {threadIndex}, isDet = {isDet}, pipe = {pipeSize}");
            for (int eIndex = 0; eIndex < Constants.numEpoch; eIndex++)
            {
                int dist_numEmit = 0;
                int dist_numCommit = 0;
                var dist_latency = new List<double>();
                var dist_prepareTxnTime = new List<double>();     // grain receive txn  ==>  start execute txn
                var dist_executeTxnTime = new List<double>();     // start execute txn  ==>  finish execute txn
                var dist_commitTxnTime = new List<double>();      // finish execute txn ==>  batch committed

                int non_dist_numEmit = 0;
                int non_dist_numCommit = 0;
                var non_dist_latency = new List<double>();
                var non_dist_prepareTxnTime = new List<double>();     // grain receive txn  ==>  start execute txn
                var non_dist_executeTxnTime = new List<double>();     // start execute txn  ==>  finish execute txn
                var non_dist_commitTxnTime = new List<double>();      // finish execute txn ==>  batch committed

                // only for ACT
                int dist_numDeadlock = 0;
                int dist_numNotSerializable = 0;
                int dist_numNotSureSerializable = 0;

                int non_dist_numDeadlock = 0;
                int non_dist_numNotSerializable = 0;
                int non_dist_numNotSureSerializable = 0;

                var tasks = new List<Task<TransactionResult>>();
                var reqs = new Dictionary<Task<TransactionResult>, Tuple<DateTime, bool>>();
                var queue = thread_requests[eIndex][threadIndex];
                RequestData txn;
                await Task.Delay(TimeSpan.FromMilliseconds(500));   // give some time for producer to populate the buffer

                //Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                globalWatch.Restart();
                var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                do
                {
                    while (tasks.Count < pipeSize && queue.TryDequeue(out txn))
                    {
                        var startTxnTime = DateTime.Now;
                        var newTask = benchmark.NewTransaction(client, txn);

                        if (txn.isDistTxn) dist_numEmit++;
                        else non_dist_numEmit++;

                        reqs.Add(newTask, new Tuple<DateTime, bool>(startTxnTime, txn.isDistTxn));
                        tasks.Add(newTask);
                    }
                    if (tasks.Count != 0)
                    {
                        var task = await Task.WhenAny(tasks);
                        var endTxnTime = DateTime.Now;
                        var noException = true;
                        try
                        {
                            //Needed to catch exception of individual task (not caught by Snapper's exception) which would not be thrown by WhenAny
                            await task;
                        }
                        catch (Exception e)    // this exception is only related to OrleansTransaction
                        {
                            Console.WriteLine($"Exception:{e.Message}, {e.StackTrace}");
                            noException = false;
                        }
                        if (noException)
                        {
                            var isDistTxn = reqs[task].Item2;

                            if (task.Result.exception == false)
                            {
                                if (isDistTxn)
                                {
                                    dist_numCommit++;
                                    dist_latency.Add((endTxnTime - reqs[task].Item1).TotalMilliseconds);
                                    dist_prepareTxnTime.Add(task.Result.prepareTime);
                                    dist_executeTxnTime.Add(task.Result.executeTime);
                                    dist_commitTxnTime.Add(task.Result.commitTime);
                                }
                                else
                                {
                                    non_dist_numCommit++;
                                    non_dist_latency.Add((endTxnTime - reqs[task].Item1).TotalMilliseconds);
                                    non_dist_prepareTxnTime.Add(task.Result.prepareTime);
                                    non_dist_executeTxnTime.Add(task.Result.executeTime);
                                    non_dist_commitTxnTime.Add(task.Result.commitTime);
                                }
                            }
                            else
                            {
                                if (isDistTxn)
                                {
                                    if (task.Result.Exp_Serializable) dist_numNotSerializable++;
                                    else if (task.Result.Exp_NotSureSerializable) dist_numNotSureSerializable++;
                                    else if (task.Result.Exp_Deadlock) dist_numDeadlock++;
                                }
                                else
                                {
                                    if (task.Result.Exp_Serializable) non_dist_numNotSerializable++;
                                    else if (task.Result.Exp_NotSureSerializable) non_dist_numNotSureSerializable++;
                                    else if (task.Result.Exp_Deadlock) non_dist_numDeadlock++;
                                }
                            }
                        }
                        tasks.Remove(task);
                        reqs.Remove(task);
                    }
                }
                while (globalWatch.ElapsedMilliseconds < Constants.epochDurationMSecs && (queue.Count != 0 || !isProducerFinish[eIndex]));
                isEpochFinish[eIndex] = true;   // which means producer doesn't need to produce more requests

                //Wait for the tasks exceeding epoch time and also count them into results
                while (tasks.Count != 0)
                {
                    var task = await Task.WhenAny(tasks);
                    var endTxnTime = DateTime.Now;
                    bool noException = true;
                    try
                    {
                        await task;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Exception: {e.Message}. ");
                        noException = false;
                    }
                    if (noException)
                    {
                        var isDistTxn = reqs[task].Item2;

                        if (task.Result.exception == false)
                        {
                            if (isDistTxn)
                            {
                                dist_numCommit++;
                                dist_latency.Add((endTxnTime - reqs[task].Item1).TotalMilliseconds);
                                dist_prepareTxnTime.Add(task.Result.prepareTime);
                                dist_executeTxnTime.Add(task.Result.executeTime);
                                dist_commitTxnTime.Add(task.Result.commitTime);
                            }
                            else
                            {
                                non_dist_numCommit++;
                                non_dist_latency.Add((endTxnTime - reqs[task].Item1).TotalMilliseconds);
                                non_dist_prepareTxnTime.Add(task.Result.prepareTime);
                                non_dist_executeTxnTime.Add(task.Result.executeTime);
                                non_dist_commitTxnTime.Add(task.Result.commitTime);
                            }
                        }
                        else
                        {
                            if (isDistTxn)
                            {
                                if (task.Result.Exp_Serializable) dist_numNotSerializable++;
                                else if (task.Result.Exp_NotSureSerializable) dist_numNotSureSerializable++;
                                else if (task.Result.Exp_Deadlock) dist_numDeadlock++;
                            }
                            else
                            {
                                if (task.Result.Exp_Serializable) non_dist_numNotSerializable++;
                                else if (task.Result.Exp_NotSureSerializable) non_dist_numNotSureSerializable++;
                                else if (task.Result.Exp_Deadlock) non_dist_numDeadlock++;
                            }
                        }
                    }
                    tasks.Remove(task);
                    reqs.Remove(task);
                }
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                globalWatch.Stop();
                //thread_requests[eIndex].Remove(threadIndex);

                if (isDet)
                    Console.WriteLine($"PACT: dist_tp = {1000 * dist_numCommit / (endTime - startTime)}, " +
                                            $"non_dist_tp = {1000 * non_dist_numCommit / (endTime - startTime)}");
                else
                {
                    if (workload.pactPercent > 0)
                    {
                        Console.WriteLine($"ACT: dist_tp = {1000 * dist_numCommit / (endTime - startTime)}, " +
                                            $"dist_abort = {dist_numEmit - dist_numCommit}, dist_numDeadlock = {dist_numDeadlock}, dist_numNotSerializable = {dist_numNotSerializable}, dist_numNotSureSerializable = {dist_numNotSureSerializable}, " +
                                            $"non_dist_tp = {1000 * non_dist_numCommit / (endTime - startTime)} " +
                                            $"non_dist_abort = {non_dist_numEmit - non_dist_numCommit}, non_dist_numDeadlock = {non_dist_numDeadlock}, non_dist_numNotSerializable = {non_dist_numNotSerializable}, non_dist_numNotSureSerializable = {non_dist_numNotSureSerializable}");
                    }
                    else
                    {
                        Console.WriteLine($"ACT: dist_tp = {1000 * dist_numCommit / (endTime - startTime)}, dist_abort% = {Helper.ChangeFormat((dist_numEmit - dist_numCommit) * 100.0 / dist_numEmit, 2)}%, " +
                                               $"non_dist_tp = {1000 * non_dist_numCommit / (endTime - startTime)}, non_dist_abort% = {Helper.ChangeFormat((non_dist_numEmit - non_dist_numCommit) * 100.0 / non_dist_numEmit, 2)}%");
                    }
                } 

                var res = new WorkloadResult();
                res.SetTime(startTime, endTime);
                res.SetNumber(isDet, true, dist_numCommit, dist_numEmit, dist_numDeadlock, dist_numNotSerializable, dist_numNotSureSerializable);
                res.SetNumber(isDet, false, non_dist_numCommit, non_dist_numEmit, non_dist_numDeadlock, non_dist_numNotSerializable, non_dist_numNotSureSerializable);
                res.SetLatency(isDet, true, dist_latency, dist_prepareTxnTime, dist_executeTxnTime, dist_commitTxnTime);
                res.SetLatency(isDet, false, non_dist_latency, non_dist_prepareTxnTime, non_dist_executeTxnTime, non_dist_commitTxnTime);
                results[threadIndex] = res;
                threadAcks[eIndex].Signal();  // Signal the completion of epoch
            }
        }

        static async void Initialize()
        {
            numDetConsumer = Constants.numCPUPerSilo / 2;
            numNonDetConsumer = Constants.numCPUPerSilo / 2;
            if (workload.pactPercent == 100) numNonDetConsumer = 0;
            else if (workload.pactPercent == 0) numDetConsumer = 0;

            switch (Constants.benchmark)
            {
                case BenchmarkType.SMALLBANK:
                    benchmarks = new SmallBankBenchmark[numDetConsumer + numNonDetConsumer];
                    for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++) benchmarks[i] = new SmallBankBenchmark();
                    break;
                case BenchmarkType.TPCC:
                    benchmarks = new TPCCBenchmark[numDetConsumer + numNonDetConsumer];
                    for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++) benchmarks[i] = new TPCCBenchmark();
                    break;
                default:
                    throw new Exception("Exception: SnapperExperimentProcess only support SmallBank and TPCC benchmarks");
            }
            results = new WorkloadResult[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                if (i < numDetConsumer) benchmarks[i].GenerateBenchmark(workload, true);
                else benchmarks[i].GenerateBenchmark(workload, false);
            }

            // some initialization for generating workload
            if (detBufferSize == 0 && nonDetBufferSize == 0)
            {
                if (numDetConsumer > 0) detBufferSize = workload.pactPipeSize * 10;
                if (numNonDetConsumer > 0) nonDetBufferSize = workload.actPipeSize * 10;
            }
            Console.WriteLine($"detPercent = {workload.pactPercent}%, detBuffer = {detBufferSize}, nonDetBuffer = {nonDetBufferSize}");
            shared_requests = new Dictionary<int, Queue<Tuple<bool, RequestData>>>();   // <epoch, <producerID, <isDet, grainIDs>>>
            for (int epoch = 0; epoch < Constants.numEpoch; epoch++) shared_requests.Add(epoch, new Queue<Tuple<bool, RequestData>>());

            workloadGenerator = new WorkloadGenerator(workerID, workload, shared_requests);
            workloadGenerator.GenerateWorkload();

            InitializeProducerThread();
            await InitializeClients();
            InitializeConsumerThreads();
            initializationDone = true;
        }

        static void ProducerThreadWork()
        {
            isEpochFinish = new bool[Constants.numEpoch];
            isProducerFinish = new bool[Constants.numEpoch];
            for (int e = 0; e < Constants.numEpoch; e++)
            {
                isEpochFinish[e] = false;  // when worker thread finishes an epoch, set true
                isProducerFinish[e] = false;
            }
            for (int eIndex = 0; eIndex < Constants.numEpoch; eIndex++)
            {
                var producer_queue = shared_requests[eIndex];
                var start = producer_queue.Count;
                while (producer_queue.Count > 0 && !isEpochFinish[eIndex])
                {
                    var txn = producer_queue.Dequeue();
                    var isDet = txn.Item1;
                    var isConsumed = false;
                    if (isDet)     // keep checking detThread until the txn is put to the consumer buffer
                    {
                        while (!isConsumed && !isEpochFinish[eIndex])
                        {
                            for (int detThread = 0; detThread < numDetConsumer; detThread++)
                            {
                                if (thread_requests[eIndex][detThread].Count < detBufferSize)
                                {
                                    thread_requests[eIndex][detThread].Enqueue(txn.Item2);
                                    isConsumed = true;
                                    break;
                                }
                            }
                            if (isConsumed) break;
                        }
                    }
                    else   // keep checking nonDet thread until the txn is consumed by the consumerThread
                    {
                        while (!isConsumed && !isEpochFinish[eIndex])
                        {
                            for (int nonDetThread = numDetConsumer; nonDetThread < numDetConsumer + numNonDetConsumer; nonDetThread++)
                            {
                                if (thread_requests[eIndex][nonDetThread].Count < nonDetBufferSize)
                                {
                                    thread_requests[eIndex][nonDetThread].Enqueue(txn.Item2);
                                    isConsumed = true;
                                    break;
                                }
                            }
                            if (isConsumed) break;
                        }
                    }
                }

                var det = 0;
                var nonDet = 0;
                foreach (var txn in producer_queue)
                {
                    if (txn.Item1) det++;
                    else nonDet++;
                }

                isProducerFinish[eIndex] = true;   // when Count == 0, set true
                shared_requests.Remove(eIndex);
            }
        }

        static void InitializeProducerThread()
        {
            barriers = new Barrier[Constants.numEpoch];
            threadAcks = new CountdownEvent[Constants.numEpoch];
            for (int i = 0; i < Constants.numEpoch; i++)
            {
                barriers[i] = new Barrier(numDetConsumer + numNonDetConsumer + 1);
                threadAcks[i] = new CountdownEvent(numDetConsumer + numNonDetConsumer);
            }

            thread_requests = new Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>>();
            for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
            {
                thread_requests.Add(epoch, new Dictionary<int, ConcurrentQueue<RequestData>>());
                for (int t = 0; t < numDetConsumer + numNonDetConsumer; t++) thread_requests[epoch].Add(t, new ConcurrentQueue<RequestData>());
            }

            producerThread = new Thread(ProducerThreadWork);
            producerThread.Start();
        }

        static async Task InitializeClients()
        {
            var manager = new OrleansClientManager();
            clients = new IClusterClient[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
                clients[i] = await manager.StartOrleansClient();
        }

        static void InitializeConsumerThreads()
        {
            //Spawn Threads        
            concumerThreads = new Thread[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                var thread = new Thread(ThreadWorkAsync);
                concumerThreads[i] = thread;
                if (i < numDetConsumer) thread.Start(new Tuple<int, bool>(i, true));
                else thread.Start(new Tuple<int, bool>(i, false));
            }
        }
    }
}