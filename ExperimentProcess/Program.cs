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

namespace ExperimentProcess
{
    static class Program
    {
        // for communication between ExpController and ExpProcess
        static PushSocket sink;
        static string sinkAddress;
        static string controllerAddress;
        static ISerializer serializer;
        static CountdownEvent[] threadAcks;
        static WorkloadConfiguration workload;
        static bool initializationDone = false;

        static int numProducer;
        static int numDetConsumer;
        static int numNonDetConsumer;
        static int detBufferSize;
        static int nonDetBufferSize;
        static Thread[] threads;         // used to submit transaction requests
        static Barrier[] barriers;
        static IClusterClient[] clients;
        static IBenchmark[] benchmarks;
        static bool[] isEpochFinish;
        static bool[] isProducerFinish;
        static Dictionary<int, Queue<Tuple<bool, RequestData>>> shared_requests;                // <epoch, <isDet, grainIDs>>
        static Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>> thread_requests;  // <epoch, <consumerID, grainIDs>>

        static WorkloadGenerator workloadGenerator;
        static WorkloadResult[] results;
        static ExperimentResultAggregator resultAggregator;

        static void Main()
        {
            if (Constants.numWorker > 1)
            {
                sinkAddress = Constants.worker_Remote_SinkAddress;
                controllerAddress = Constants.worker_Remote_ControllerAddress;
            }
            else
            {
                sinkAddress = Constants.worker_Local_SinkAddress;
                controllerAddress = Constants.worker_Local_ControllerAddress;
            }
            sink = new PushSocket(sinkAddress);
            serializer = new MsgPackSerializer();

            resultAggregator = new ExperimentResultAggregator();

            Console.WriteLine("Worker is Started...");
            ProcessWork();
            //Console.ReadLine();
        }

        static void ProcessWork()
        {
            Console.WriteLine("====== WORKER ======");
            using (var controller = new SubscriberSocket(controllerAddress))
            {
                Console.WriteLine($"worker is ready to connect controller");
                controller.Subscribe("WORKLOAD_INIT");
                //Acknowledge the controller thread
                var msg = new NetworkMessage(Utilities.MsgType.WORKER_CONNECT);
                sink.SendFrame(serializer.serialize(msg));
                Console.WriteLine("Connected to controller");

                controller.Options.ReceiveHighWatermark = 1000;
                var messageTopicReceived = controller.ReceiveFrameString();
                var messageReceived = controller.ReceiveFrameBytes();
                //Wait to receive workload msg
                msg = serializer.deserialize<NetworkMessage>(messageReceived);
                Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT);
                Console.WriteLine("Receive workload configuration.");
                controller.Unsubscribe("WORKLOAD_INIT");
                controller.Subscribe("RUN_EPOCH");
                workload = serializer.deserialize<WorkloadConfiguration>(msg.contents);
                Console.WriteLine("Received workload message from controller");
                Console.WriteLine($"detPipe per thread = {workload.pactPipeSize}, nonDetPipe per thread = {workload.actPipeSize}");

                //Initialize threads and other data-structures for epoch runs
                Initialize();
                while (!initializationDone) Thread.Sleep(100);

                Console.WriteLine("Finished initialization, sending ACK to controller");
                //Send an ACK
                msg = new NetworkMessage(Utilities.MsgType.WORKLOAD_INIT_ACK);
                sink.SendFrame(serializer.serialize(msg));

                for (int i = 0; i < workload.numEpochs; i++)
                {
                    messageTopicReceived = controller.ReceiveFrameString();
                    messageReceived = controller.ReceiveFrameBytes();
                    //Wait for EPOCH RUN signal
                    msg = serializer.deserialize<NetworkMessage>(messageReceived);
                    Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH);
                    //Console.WriteLine($"Received signal from controller. Running epoch {i} across {numDetConsumer + numNonDetConsumer} worker threads");
                    //Signal the barrier
                    barriers[i].SignalAndWait();
                    //Wait for all threads to finish the epoch
                    threadAcks[i].Wait();
                    var result = resultAggregator.AggregateResultForEpoch(results);
                    msg = new NetworkMessage(Utilities.MsgType.RUN_EPOCH_ACK);
                    msg.contents = serializer.serialize(result);
                    sink.SendFrame(serializer.serialize(msg));
                }

                Console.WriteLine("Finished running epochs, exiting");
                foreach (var thread in threads) thread.Join();
            }
        }

        static async void Initialize()
        {
            numProducer = 1;
            numDetConsumer = Constants.numCPUPerSilo / Constants.numCPUBasic;
            numNonDetConsumer = Constants.numCPUPerSilo / Constants.numCPUBasic;
            if (workload.pactPercent == 100) numNonDetConsumer = 0;
            else if (workload.pactPercent == 0) numDetConsumer = 0;

            switch (workload.benchmark)
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
                    throw new Exception("Exception: ExperimentProcess only support SmallBank and TPCC benchmarks");
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
            for (int epoch = 0; epoch < workload.numEpochs; epoch++) shared_requests.Add(epoch, new Queue<Tuple<bool, RequestData>>());

            workloadGenerator = new WorkloadGenerator(workload, shared_requests);
            workloadGenerator.GenerateWorkload();

            InitializeProducerThread();
            await InitializeClients();
            InitializeConsumerThreads();
            initializationDone = true;
        }

        static void ProducerThreadWork(object obj)
        {
            isEpochFinish = new bool[workload.numEpochs];
            isProducerFinish = new bool[workload.numEpochs];
            for (int e = 0; e < workload.numEpochs; e++)
            {
                isEpochFinish[e] = false;  // when worker thread finishes an epoch, set true
                isProducerFinish[e] = false;
            }
            for (int eIndex = 0; eIndex < workload.numEpochs; eIndex++)
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
                //Console.WriteLine($"end: shared_requests[{eIndex}].count = {start} --> {producer_queue.Count}, det = {det}, nondet = {nonDet}");
                isProducerFinish[eIndex] = true;   // when Count == 0, set true
                shared_requests.Remove(eIndex);
            }
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
            for (int eIndex = 0; eIndex < workload.numEpochs; eIndex++)
            {
                int numEmit = 0;
                int numDetCommit = 0;
                int numNonDetCommit = 0;
                int numOrleansTxnEmit = 0;
                int numNonDetTransaction = 0;
                int numDeadlock = 0;
                int numNotSerializable = 0;
                int numNotSureSerializable = 0;
                var latencies = new List<double>();
                var det_latencies = new List<double>();
                var tasks = new List<Task<TransactionResult>>();
                var reqs = new Dictionary<Task<TransactionResult>, TimeSpan>();
                var queue = thread_requests[eIndex][threadIndex];
                RequestData txn;
                await Task.Delay(TimeSpan.FromMilliseconds(500));   // give some time for producer to populate the buffer

                //Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                globalWatch.Restart();
                long startTime = 0;
                startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                do
                {
                    while (tasks.Count < pipeSize && queue.TryDequeue(out txn))
                    {
                        var asyncReqStartTime = globalWatch.Elapsed;
                        var newTask = benchmark.NewTransaction(client, txn);
                        numEmit++;
                        reqs.Add(newTask, asyncReqStartTime);
                        tasks.Add(newTask);
                    }
                    if (tasks.Count != 0)
                    {
                        var task = await Task.WhenAny(tasks);
                        var asyncReqEndTime = globalWatch.Elapsed;
                        bool noException = true;
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
                        numOrleansTxnEmit++;
                        if (noException)
                        {
                            if (isDet)   // for det
                            {
                                if (workload.benchmark == BenchmarkType.SMALLBANK) Debug.Assert(!task.Result.exception);
                                numDetCommit++;
                                det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                            }
                            else    // for non-det + eventual + orleans txn
                            {
                                numNonDetTransaction++;
                                if (!task.Result.exception)
                                {
                                    numNonDetCommit++;
                                    var totalTime = (asyncReqEndTime - reqs[task]).TotalMilliseconds;
                                    latencies.Add(totalTime);
                                }
                                else if (task.Result.Exp_Serializable) numNotSerializable++;
                                else if (task.Result.Exp_NotSureSerializable) numNotSureSerializable++;
                                else if (task.Result.Exp_Deadlock) numDeadlock++;
                            }
                        }
                        tasks.Remove(task);
                        reqs.Remove(task);
                    }
                }
                //while (numEmit < numTxn);
                while (globalWatch.ElapsedMilliseconds < workload.epochDurationMSecs && (queue.Count != 0 || !isProducerFinish[eIndex]));
                isEpochFinish[eIndex] = true;   // which means producer doesn't need to produce more requests

                //Wait for the tasks exceeding epoch time and also count them into results
                while (tasks.Count != 0)
                {
                    var task = await Task.WhenAny(tasks);
                    var asyncReqEndTime = globalWatch.Elapsed;
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
                    numOrleansTxnEmit++;
                    if (noException)
                    {
                        if (isDet)   // for det
                        {
                            Debug.Assert(!task.Result.exception);
                            numDetCommit++;
                            det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                        }
                        else    // for non-det + eventual + orleans txn
                        {
                            numNonDetTransaction++;
                            if (!task.Result.exception)
                            {
                                numNonDetCommit++;
                                var totalTime = (asyncReqEndTime - reqs[task]).TotalMilliseconds;
                                latencies.Add(totalTime);
                            }
                            else if (task.Result.Exp_Serializable) numNotSerializable++;
                            else if (task.Result.Exp_NotSureSerializable) numNotSureSerializable++;
                            else if (task.Result.Exp_Deadlock) numDeadlock++;
                        }
                    }
                    tasks.Remove(task);
                    reqs.Remove(task);
                }
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                globalWatch.Stop();
                thread_requests[eIndex].Remove(threadIndex);
                if (isDet) Console.WriteLine($"det-commit = {numDetCommit}, tp = {1000 * numDetCommit / (endTime - startTime)}. ");
                else
                {
                    if (Constants.implementationType == ImplementationType.ORLEANSTXN) Console.WriteLine($"total_num_nondet = {numOrleansTxnEmit}, nondet-commit = {numNonDetCommit}");
                    else Console.WriteLine($"total_num_nondet = {numNonDetTransaction}, nondet-commit = {numNonDetCommit}, tp = {1000 * numNonDetCommit / (endTime - startTime)}, Deadlock = {numDeadlock}, NotSerilizable = {numNotSerializable}, NotSureSerializable = {numNotSureSerializable}");
                }
                WorkloadResult res;
                if (Constants.implementationType == ImplementationType.ORLEANSTXN) res = new WorkloadResult(numDetCommit, numOrleansTxnEmit, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numNotSureSerializable, numDeadlock);
                else res = new WorkloadResult(numDetCommit, numNonDetTransaction, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numNotSureSerializable, numDeadlock);
                res.setLatency(latencies, det_latencies);
                results[threadIndex] = res;
                threadAcks[eIndex].Signal();  // Signal the completion of epoch
            }
        }

        static void InitializeProducerThread()
        {
            barriers = new Barrier[workload.numEpochs];
            threadAcks = new CountdownEvent[workload.numEpochs];
            for (int i = 0; i < workload.numEpochs; i++)
            {
                barriers[i] = new Barrier(numDetConsumer + numNonDetConsumer + 1);
                threadAcks[i] = new CountdownEvent(numDetConsumer + numNonDetConsumer);
            }

            thread_requests = new Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>>();
            for (int epoch = 0; epoch < workload.numEpochs; epoch++)
            {
                thread_requests.Add(epoch, new Dictionary<int, ConcurrentQueue<RequestData>>());
                for (int t = 0; t < numDetConsumer + numNonDetConsumer; t++) thread_requests[epoch].Add(t, new ConcurrentQueue<RequestData>());
            }
            for (int producer = 0; producer < numProducer; producer++)
            {
                var thread = new Thread(ProducerThreadWork);
                thread.Start(producer);
            }
        }

        static async Task InitializeClients()
        {
            clients = new IClusterClient[numDetConsumer + numNonDetConsumer];
            var clientConfig = new ClientConfiguration();
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                if (Constants.localCluster) clients[i] = await clientConfig.StartClientWithRetries();
                else clients[i] = await clientConfig.StartClientWithRetriesToCluster();
            }
        }

        static void InitializeConsumerThreads()
        {
            //Spawn Threads        
            threads = new Thread[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                var thread = new Thread(ThreadWorkAsync);
                threads[i] = thread;
                if (i < numDetConsumer) thread.Start(new Tuple<int, bool>(i, true));
                else thread.Start(new Tuple<int, bool>(i, false));
            }
        }
    }
}