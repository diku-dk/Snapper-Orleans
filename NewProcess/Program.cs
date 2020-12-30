using NetMQ;
using System;
using Orleans;
using Utilities;
using System.Linq;
using NetMQ.Sockets;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Collections.Concurrent;
using MathNet.Numerics.Distributions;

namespace NewProcess
{
    class Program
    {
        static string sinkAddress;
        static string controllerAddress;
        static IClusterClient[] clients;
        static PushSocket sink;
        static Thread[] threads;
        static Barrier[] barriers;
        static ISerializer serializer;
        static IBenchmark[] benchmarks;
        static WorkloadResults[] results;
        static CountdownEvent[] threadAcks;
        static WorkloadConfiguration config;
        static bool initializationDone = false;
        static IDiscreteDistribution detDistribution = new DiscreteUniform(0, 99, new Random());
        static Dictionary<int, Queue<Tuple<bool, List<int>>>> shared_requests;  // <epoch, <isDet, grainIDs>>
        static Dictionary<int, Dictionary<int, ConcurrentQueue<List<int>>>> thread_requests;     // <epoch, <consumerID, grainIDs>>

        static int siloCPU;
        static int detPercent;
        static bool[] isEpochFinish;
        static bool[] isProducerFinish;
        static int detPipeSize;
        static int nonDetPipeSize;
        static int detBufferSize;
        static int nonDetBufferSize;
        static int numDetConsumer;
        static int numNonDetConsumer;
        static int numProducer;

        // parameters for hot record
        static double skewness;
        static double hotGrainRatio;

        private static void ProducerThreadWork(object obj)
        {
            isEpochFinish = new bool[config.numEpochs];
            isProducerFinish = new bool[config.numEpochs];
            for (int e = 0; e < config.numEpochs; e++)
            {
                isEpochFinish[e] = false;  // when worker thread finishes an epoch, set true
                isProducerFinish[e] = false;
            } 
            for (int eIndex = 0; eIndex < config.numEpochs; eIndex++)
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
                Console.WriteLine($"end: shared_requests[{eIndex}].count = {start} --> {producer_queue.Count}, det = {det}, nondet = {nonDet}");
                isProducerFinish[eIndex] = true;   // when Count == 0, set true
                shared_requests.Remove(eIndex);
            }
        }

        private static async void ThreadWorkAsync(object obj)
        {
            var input = (Tuple<int, bool>)obj;
            int threadIndex = input.Item1;
            var isDet = input.Item2;
            var pipeSize = isDet ? detPipeSize : nonDetPipeSize;
            var globalWatch = new Stopwatch();
            var benchmark = benchmarks[threadIndex];
            var client = clients[threadIndex % (numDetConsumer + numNonDetConsumer)];
            Console.WriteLine($"thread = {threadIndex}, isDet = {isDet}, pipe = {pipeSize}");
            for (int eIndex = 0; eIndex < config.numEpochs; eIndex++)
            {
                int numEmit = 0;
                int numDetCommit = 0;
                int numNonDetCommit = 0;
                int numDetTransaction = 0;
                int numNonDetTransaction = 0;
                int numDeadlock = 0;
                int numNotSerializable = 0;
                var latencies = new List<double>();
                var det_latencies = new List<double>();
                var tasks = new List<Task<TransactionResult>>();
                var reqs = new Dictionary<Task<TransactionResult>, TimeSpan>();
                var queue = thread_requests[eIndex][threadIndex];
                List<int> txn = null;
                await Task.Delay(TimeSpan.FromMilliseconds(100));   // give some time for producer to populate the buffer
                //Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                globalWatch.Restart();
                var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                do
                {
                    while (tasks.Count < pipeSize && queue.TryDequeue(out txn))
                    //while (tasks.Count < pipeSize && queue.TryDequeue(out txn) && numEmit < numTxn)
                    {
                        var asyncReqStartTime = globalWatch.Elapsed;
                        var newTask = benchmark.newTransaction(client, txn);
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
                        if (noException)
                        {
                            if (task.Result.isDet)   // for det
                            {
                                numDetTransaction++;
                                if (!task.Result.exception)
                                {
                                    numDetCommit++;
                                    det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                                }
                            }
                            else    // for non-det + eventual + orleans txn
                            {
                                numNonDetTransaction++;
                                if (!task.Result.exception)
                                {
                                    numNonDetCommit++;
                                    latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                                }
                                else if (task.Result.Exp_Serializable) numNotSerializable++;
                                else if (task.Result.Exp_Deadlock) numDeadlock++;
                            }
                        }
                        tasks.Remove(task);
                        reqs.Remove(task);
                    }
                }
                //while (numEmit < numTxn);
                while (globalWatch.ElapsedMilliseconds < config.epochDurationMSecs && (queue.Count != 0 || !isProducerFinish[eIndex]));
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
                    if (noException)
                    {
                        if (task.Result.isDet)   // for det + eventual + orleans txn
                        {
                            numDetTransaction++;
                            if (!task.Result.exception)
                            {
                                numDetCommit++;
                                det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                            }
                        }
                        else    // for non-det 
                        {
                            numNonDetTransaction++;
                            if (!task.Result.exception)
                            {
                                numNonDetCommit++;
                                latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
                            }
                            else if (task.Result.Exp_Serializable) numNotSerializable++;
                            else if (task.Result.Exp_Deadlock) numDeadlock++;
                        }
                    }
                    tasks.Remove(task);
                    reqs.Remove(task);
                }
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                globalWatch.Stop();
                Console.WriteLine($"thread_requests[{eIndex}][{threadIndex}] has {thread_requests[eIndex][threadIndex].Count} txn remaining");
                thread_requests[eIndex].Remove(threadIndex);
                if (isDet) Console.WriteLine($"total_num_det = {numDetTransaction}, det-commit = {numDetCommit}, tp = {1000 * numDetCommit / (endTime - startTime)}. ");
                else Console.WriteLine($"total_num_nondet = {numNonDetTransaction}, nondet-commit = {numNonDetCommit}, tp = {1000 * numNonDetCommit / (endTime - startTime)}, Deadlock = {numDeadlock}, NotSerilizable = {numNotSerializable}");
                WorkloadResults res;
                if (config.grainImplementationType == ImplementationType.SNAPPER)
                    res = new WorkloadResults(numDetTransaction, numNonDetTransaction, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numDeadlock);
                else res = new WorkloadResults(numDetTransaction, numEmit, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numDeadlock);
                res.setLatency(latencies, det_latencies);
                results[threadIndex] = res;
                threadAcks[eIndex].Signal();  //Signal the completion of epoch
            }
        }

        private static async void Initialize()
        {
            numProducer = 1;
            detPercent = (int)config.deterministicTxnPercent;
            numDetConsumer = 1;
            numNonDetConsumer = 1;
            if (detPercent == 100) numNonDetConsumer = 0;
            else if (detPercent == 0) numDetConsumer = 0;

            if (config.benchmark != BenchmarkType.SMALLBANK) throw new Exception("Exception: NewProcess only support SmallBank benchmark");
            benchmarks = new SmallBankBenchmark[numDetConsumer + numNonDetConsumer];
            results = new WorkloadResults[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                benchmarks[i] = new SmallBankBenchmark();
                if (i < numDetConsumer) benchmarks[i].generateBenchmark(config, true);
                else benchmarks[i].generateBenchmark(config, false);
            }
            
            InitializeWorkload();
            InitializeProducerThread();
            await InitializeClients();
            InitializeConsumerThreads();
            initializationDone = true;
        }

        private static void InitializeWorkload()
        {
            if (config.mixture.Sum() > 0) throw new Exception("Exception: NewProcess only support MultiTransfer");

            if (detBufferSize == 0 && nonDetBufferSize == 0)
            {
                if (numDetConsumer > 0) detBufferSize = detPercent * 100 * siloCPU / (4 * numDetConsumer) ;
                if (numNonDetConsumer > 0) nonDetBufferSize = (100 - detPercent) * 100 * siloCPU / (4 * numNonDetConsumer);
            }
            Console.WriteLine($"detPercent = {detPercent}%, detBuffer = {detBufferSize}, nonDetBuffer = {nonDetBufferSize}");
            shared_requests = new Dictionary<int, Queue<Tuple<bool, List<int>>>>();   // <epoch, <producerID, <isDet, grainIDs>>>
            for (int epoch = 0; epoch < config.numEpochs; epoch++) shared_requests.Add(epoch, new Queue<Tuple<bool, List<int>>>());

            var numTxnPerEpoch = 150000 * siloCPU / 4;
            var numGrain = config.numAccounts / config.numAccountsPerGroup;
            var numGrainPerTxn = config.numGrainsMultiTransfer;
            switch (config.distribution)
            {
                case Distribution.UNIFORM:
                    Console.WriteLine($"Generate UNIFORM data..");
                    var dist = new DiscreteUniform(0, numGrain - 1, new Random());
                    for (int epoch = 0; epoch < config.numEpochs; epoch++)
                    {
                        for (int txn = 0; txn < numTxnPerEpoch; txn++)
                        {
                            var grainsPerTxn = new List<int>();
                            for (int i = 0; i < numGrainPerTxn; i++)
                            {
                                var grain = dist.Sample();
                                while (grainsPerTxn.Contains(grain)) grain = dist.Sample();
                                grainsPerTxn.Add(grain);
                            }
                            shared_requests[epoch].Enqueue(new Tuple<bool, List<int>>(isDet(), grainsPerTxn));
                        }
                    }
                    break;
                case Distribution.HOTRECORD:
                    int numHotGrain = (int)(skewness * numGrain);
                    if (skewness == 0) hotGrainRatio = 0;
                    var numHotGrainPerTxn = hotGrainRatio * numGrainPerTxn;
                    Console.WriteLine($"Generate data for HOTRECORD, {numHotGrain} hot grains, {numHotGrainPerTxn} hot grain per txn...");
                    for (int epoch = 0; epoch < config.numEpochs; epoch++)
                    {
                        var normal_dist = new DiscreteUniform(numHotGrain, numGrain - 1, new Random());
                        DiscreteUniform hot_dist = null;
                        if (skewness != 0) hot_dist = new DiscreteUniform(0, numHotGrain - 1, new Random());
                        for (int txn = 0; txn < numTxnPerEpoch; txn++)
                        {
                            var grainsPerTxn = new List<int>();
                            for (int normal = 0; normal < numGrainPerTxn - numHotGrainPerTxn; normal++)
                            {
                                var normalGrain = normal_dist.Sample();
                                while (grainsPerTxn.Contains(normalGrain)) normalGrain = normal_dist.Sample();
                                grainsPerTxn.Add(normalGrain);
                            }
                            for (int hot = 0; hot < numHotGrainPerTxn; hot++)
                            {
                                var hotGrain = hot_dist.Sample();
                                while (grainsPerTxn.Contains(hotGrain)) hotGrain = hot_dist.Sample();
                                grainsPerTxn.Add(hotGrain);
                            }
                            shared_requests[epoch].Enqueue(new Tuple<bool, List<int>>(isDet(), grainsPerTxn));
                        }
                    }
                    break;
                case Distribution.ZIPFIAN:    // read data from file
                    Console.WriteLine($"read data from files");
                    var zipf = config.zipfianConstant;
                    var prefix = Constants.dataPath + $@"MultiTransfer\{numGrainPerTxn}\{numGrain}\hybrid\MultiTransfer_{zipf}_";

                    // read data from files
                    for (int epoch = 0; epoch < config.numEpochs; epoch++)
                    {
                        string line;
                        var path = prefix + $@"{epoch}_0.txt";
                        System.IO.StreamReader file = new System.IO.StreamReader(path);
                        while ((line = file.ReadLine()) != null)
                        {
                            var grainsPerTxn = new List<int>();
                            for (int i = 0; i < config.numGrainsMultiTransfer; i++)
                            {
                                if (i > 0) line = file.ReadLine();  // the 0th line has been read by while() loop
                                var id = int.Parse(line);
                                grainsPerTxn.Add(id);
                            }
                            shared_requests[epoch].Enqueue(new Tuple<bool, List<int>>(isDet(), grainsPerTxn));
                        }
                        file.Close();
                    }
                    break;
                default:
                    throw new Exception("Exception: Unknown distribution. ");
            }
        }

        private static bool isDet()
        {
            if (detPercent == 0) return false;
            else if (detPercent == 100) return true;

            var sample = detDistribution.Sample();
            if (sample < detPercent) return true;
            else return false;
        }

        private static void InitializeProducerThread()
        {
            barriers = new Barrier[config.numEpochs];
            threadAcks = new CountdownEvent[config.numEpochs];
            for (int i = 0; i < config.numEpochs; i++)
            {
                barriers[i] = new Barrier(numDetConsumer + numNonDetConsumer + 1);
                threadAcks[i] = new CountdownEvent(numDetConsumer + numNonDetConsumer);
            }

            thread_requests = new Dictionary<int, Dictionary<int, ConcurrentQueue<List<int>>>>();
            for (int epoch = 0; epoch < config.numEpochs; epoch++)
            {
                thread_requests.Add(epoch, new Dictionary<int, ConcurrentQueue<List<int>>>());
                for (int t = 0; t < numDetConsumer + numNonDetConsumer; t++) thread_requests[epoch].Add(t, new ConcurrentQueue<List<int>>());
            }
            for (int producer = 0; producer < numProducer; producer++)
            {
                var thread = new Thread(ProducerThreadWork);
                thread.Start(producer);
            }
        }

        private static async Task InitializeClients()
        {
            clients = new IClusterClient[numDetConsumer + numNonDetConsumer];
            var clientConfig = new ClientConfiguration();
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                if (Constants.localCluster) clients[i] = await clientConfig.StartClientWithRetries();
                else clients[i] = await clientConfig.StartClientWithRetriesToCluster();
            }
        }

        private static void InitializeConsumerThreads()
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

        static void ProcessWork()
        {
            Console.WriteLine("====== WORKER ======");
            using (var controller = new SubscriberSocket(controllerAddress))
            {
                Console.WriteLine($"worker is ready to connect controller");
                controller.Subscribe("WORKLOAD_INIT");
                //Acknowledge the controller thread
                var msg = new NetworkMessageWrapper(Utilities.MsgType.WORKER_CONNECT);
                sink.SendFrame(serializer.serialize(msg));
                Console.WriteLine("Connected to controller");

                controller.Options.ReceiveHighWatermark = 1000;
                var messageTopicReceived = controller.ReceiveFrameString();
                var messageReceived = controller.ReceiveFrameBytes();
                //Wait to receive workload msg
                msg = serializer.deserialize<NetworkMessageWrapper>(messageReceived);
                Trace.Assert(msg.msgType == Utilities.MsgType.WORKLOAD_INIT);
                Console.WriteLine("Receive workload configuration.");
                controller.Unsubscribe("WORKLOAD_INIT");
                controller.Subscribe("RUN_EPOCH");
                config = serializer.deserialize<WorkloadConfiguration>(msg.contents);
                Console.WriteLine("Received workload message from controller");

                //Initialize threads and other data-structures for epoch runs
                Initialize();
                while (!initializationDone) Thread.Sleep(100);

                Console.WriteLine("Finished initialization, sending ACK to controller");
                //Send an ACK
                msg = new NetworkMessageWrapper(Utilities.MsgType.WORKLOAD_INIT_ACK);
                sink.SendFrame(serializer.serialize(msg));

                for (int i = 0; i < config.numEpochs; i++)
                {
                    messageTopicReceived = controller.ReceiveFrameString();
                    messageReceived = controller.ReceiveFrameBytes();
                    //Wait for EPOCH RUN signal
                    msg = serializer.deserialize<NetworkMessageWrapper>(messageReceived);
                    Trace.Assert(msg.msgType == Utilities.MsgType.RUN_EPOCH);
                    Console.WriteLine($"Received signal from controller. Running epoch {i} across {numDetConsumer + numNonDetConsumer} worker threads");
                    //Signal the barrier
                    barriers[i].SignalAndWait();
                    //Wait for all threads to finish the epoch
                    threadAcks[i].Wait();
                    var result = AggregateAcrossThreadsForEpoch();
                    msg = new NetworkMessageWrapper(Utilities.MsgType.RUN_EPOCH_ACK);
                    msg.contents = serializer.serialize(result);
                    sink.SendFrame(serializer.serialize(msg));
                }

                Console.WriteLine("Finished running epochs, exiting");
                foreach (var thread in threads) thread.Join();
            }
        }

        private static WorkloadResults AggregateAcrossThreadsForEpoch()
        {
            Trace.Assert(results.Length >= 1);
            int aggNumDetCommitted = results[0].numDetCommitted;
            int aggNumNonDetCommitted = results[0].numNonDetCommitted;
            int aggNumDetTransactions = results[0].numDetTxn;
            int aggNumNonDetTransactions = results[0].numNonDetTxn;
            int aggNumNotSerializable = results[0].numNotSerializable;
            int aggNumDeadlock = results[0].numDeadlock;
            long aggStartTime = results[0].startTime;
            long aggEndTime = results[0].endTime;
            var aggLatencies = new List<double>();
            var aggDetLatencies = new List<double>();
            aggLatencies.AddRange(results[0].latencies);
            aggDetLatencies.AddRange(results[0].det_latencies);
            for (int i = 1; i < results.Length; i++)    // reach thread has a result
            {
                aggNumDetCommitted += results[i].numDetCommitted;
                aggNumNonDetCommitted += results[i].numNonDetCommitted;
                aggNumDetTransactions += results[i].numDetTxn;
                aggNumNonDetTransactions += results[i].numNonDetTxn;
                aggNumNotSerializable += results[i].numNotSerializable;
                aggNumDeadlock += results[i].numDeadlock;
                aggStartTime = (results[i].startTime < aggStartTime) ? results[i].startTime : aggStartTime;
                aggEndTime = (results[i].endTime < aggEndTime) ? results[i].endTime : aggEndTime;
                aggLatencies.AddRange(results[i].latencies);
                aggDetLatencies.AddRange(results[i].det_latencies);
            }
            var res = new WorkloadResults(aggNumDetTransactions, aggNumNonDetTransactions, aggNumDetCommitted, aggNumNonDetCommitted, aggStartTime, aggEndTime, aggNumNotSerializable, aggNumDeadlock);
            res.setLatency(aggLatencies, aggDetLatencies);
            return res;
        }

        static void Main(string[] args)
        {
            if (Constants.multiWorker)
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
            serializer = new BinarySerializer();

            Console.WriteLine("Worker is Started...");

            //inject the specially required arguments into workload setting
            siloCPU = int.Parse(args[0]);
            detPipeSize = int.Parse(args[1]);
            nonDetPipeSize = int.Parse(args[2]);
            //skewness = double.Parse(args[3]);
            //hotGrainRatio = double.Parse(args[4]);
            Console.WriteLine($"detPipe per thread = {detPipeSize}, nonDetPipe per thread = {nonDetPipeSize}");
            //Console.WriteLine($"skewness = {skewness}, hotGrainRatio = {hotGrainRatio}");

            ProcessWork();
            //Console.ReadLine();
        }
    }
}