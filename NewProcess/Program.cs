using NetMQ;
using System;
using Orleans;
using Utilities;
using System.IO;
using System.Linq;
using NetMQ.Sockets;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Statistics;
using System.Collections.Concurrent;
using MathNet.Numerics.Distributions;
using static Utilities.Helper;

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
        static Dictionary<int, Queue<Tuple<bool, RequestData>>> shared_requests;  // <epoch, <isDet, grainIDs>>
        static Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>> thread_requests;     // <epoch, <consumerID, grainIDs>>

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
                await Task.Delay(TimeSpan.FromMilliseconds(100));   // give some time for producer to populate the buffer
                //Wait for all threads to arrive at barrier point
                barriers[eIndex].SignalAndWait();
                globalWatch.Restart();
                long startTime = 0;
                startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                do
                {
                    while (tasks.Count < pipeSize && queue.TryDequeue(out txn))
                    {
                        //if (numEmit == 99) startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                        var asyncReqStartTime = globalWatch.Elapsed;
                        var newTask = benchmark.newTransaction(client, txn);
                        numEmit++;
                        /*
                        if (numEmit >= 100)
                        {
                            reqs.Add(newTask, asyncReqStartTime);
                            tasks.Add(newTask);
                        }*/
                        
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
                            if (task.Result.isDet)   // for det
                            {
                                if (config.benchmark == BenchmarkType.SMALLBANK) Debug.Assert(!task.Result.exception);
                                numDetCommit++;
                                det_latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
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
                                else if (task.Result.Exp_NotSureSerializable) numNotSureSerializable++;
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
                    numOrleansTxnEmit++;
                    if (noException)
                    {
                        if (task.Result.isDet)   // for det
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
                                latencies.Add((asyncReqEndTime - reqs[task]).TotalMilliseconds);
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
                //Console.WriteLine($"thread_requests[{eIndex}][{threadIndex}] has {thread_requests[eIndex][threadIndex].Count} txn remaining");
                thread_requests[eIndex].Remove(threadIndex);
                if (isDet) Console.WriteLine($"det-commit = {numDetCommit}, tp = {1000 * numDetCommit / (endTime - startTime)}. ");
                else
                {
                    if (config.grainImplementationType == ImplementationType.ORLEANSTXN) Console.WriteLine($"total_num_nondet = {numOrleansTxnEmit}, nondet-commit = {numNonDetCommit}, tp = {1000 * numNonDetCommit / (endTime - startTime)}, Deadlock = {numDeadlock}, NotSerilizable = {numNotSerializable}, NotSureSerializable = {numNotSureSerializable}");
                    else Console.WriteLine($"total_num_nondet = {numNonDetTransaction}, nondet-commit = {numNonDetCommit}, tp = {1000 * numNonDetCommit / (endTime - startTime)}, Deadlock = {numDeadlock}, NotSerilizable = {numNotSerializable}, NotSureSerializable = {numNotSureSerializable}");
                } 
                WorkloadResults res;
                if (config.grainImplementationType == ImplementationType.ORLEANSTXN) res = new WorkloadResults(numDetCommit, numOrleansTxnEmit, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numNotSureSerializable, numDeadlock);
                else res = new WorkloadResults(numDetCommit, numNonDetTransaction, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numNotSureSerializable, numDeadlock);
                res.setLatency(latencies, det_latencies);
                results[threadIndex] = res;
                threadAcks[eIndex].Signal();  // Signal the completion of epoch
            }
        }

        private static async void Initialize()
        {
            numProducer = 1;
            detPercent = (int)config.deterministicTxnPercent;
            numDetConsumer = siloCPU / 4;
            numNonDetConsumer = siloCPU / 4;
            if (detPercent == 100) numNonDetConsumer = 0;
            else if (detPercent == 0) numDetConsumer = 0;

            switch (config.benchmark)
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
                    throw new Exception("Exception: NewProcess only support SmallBank and TPCC benchmarks");
            }
            results = new WorkloadResults[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                if (i < numDetConsumer) benchmarks[i].generateBenchmark(config, true);
                else benchmarks[i].generateBenchmark(config, false);
            }

            // some initialization for generating workload
            if (detBufferSize == 0 && nonDetBufferSize == 0)
            {
                if (numDetConsumer > 0)
                {
                    detBufferSize = detPercent * 100 * siloCPU / (4 * numDetConsumer);
                    //detBufferSize = detPipeSize * 5;
                }
                if (numNonDetConsumer > 0)
                {
                    nonDetBufferSize = (100 - detPercent) * 100 * siloCPU / (4 * numNonDetConsumer);
                    //nonDetBufferSize = nonDetPipeSize * 5;
                }
            }
            Console.WriteLine($"detPercent = {detPercent}%, detBuffer = {detBufferSize}, nonDetBuffer = {nonDetBufferSize}");
            shared_requests = new Dictionary<int, Queue<Tuple<bool, RequestData>>>();   // <epoch, <producerID, <isDet, grainIDs>>>
            for (int epoch = 0; epoch < config.numEpochs; epoch++) shared_requests.Add(epoch, new Queue<Tuple<bool, RequestData>>());

            switch (config.benchmark)
            {
                case BenchmarkType.SMALLBANK:
                    InitializeSmallBankWorkload();
                    break;
                case BenchmarkType.TPCC:
                    InitializeTPCCWorkload();
                    break;
                default:
                    throw new Exception($"Exception: Unknown benchmark {config.benchmark}");
            }

            InitializeProducerThread();
            await InitializeClients();
            InitializeConsumerThreads();
            initializationDone = true;
        }

        private static void GenerateNewOrder(int epoch)
        {
            Console.WriteLine($"Generate TPCC workload for epoch {epoch}");

            var numRound = siloCPU / 4;
            if (config.grainImplementationType == ImplementationType.ORLEANSEVENTUAL) numRound *= 3;

            var remote_count = 0;
            var txn_size = new List<int>();

            for (int round = 0; round < siloCPU / 4; round++)
            {
                DiscreteUniform hot = null;
                DiscreteUniform wh_dist = null;
                DiscreteUniform hot_wh_dist = null;
                DiscreteUniform district_dist = null;
                DiscreteUniform hot_district_dist = null;
                if (config.distribution == Distribution.HOTRECORD)
                {
                    // hot set
                    var num_hot_wh = (int)(0.5 * config.numWarehouse);
                    var num_hot_district = (int)(0.1 * Constants.NUM_D_PER_W);
                    hot_wh_dist = new DiscreteUniform(0, num_hot_wh - 1, new Random());
                    wh_dist = new DiscreteUniform(num_hot_wh, config.numWarehouse - 1, new Random());
                    hot_district_dist = new DiscreteUniform(0, num_hot_district - 1, new Random());
                    district_dist = new DiscreteUniform(num_hot_district, Constants.NUM_D_PER_W - 1, new Random());
                    hot = new DiscreteUniform(0, 99, new Random());
                }
                else
                {
                    Debug.Assert(config.distribution == Distribution.UNIFORM);
                    wh_dist = new DiscreteUniform(0, config.numWarehouse - 1, new Random());
                    district_dist = new DiscreteUniform(0, Constants.NUM_D_PER_W - 1, new Random());
                }
                var ol_cnt_dist_uni = new DiscreteUniform(5, 15, new Random());
                var rbk_dist_uni = new DiscreteUniform(1, 100, new Random());
                var local_dist_uni = new DiscreteUniform(1, 100, new Random());
                var quantity_dist_uni = new DiscreteUniform(1, 10, new Random());

                for (int txn = 0; txn < Constants.BASE_NUM_NEWORDER; txn++)
                {
                    int W_ID;
                    int D_ID;
                    if (config.distribution == Distribution.HOTRECORD)
                    {
                        var p = hot.Sample();
                        if (p < Constants.hotRatio * 100)    // 75% choose from hot set
                        {
                            W_ID = hot_wh_dist.Sample();
                            D_ID = hot_district_dist.Sample();
                        }
                        else
                        {
                            W_ID = wh_dist.Sample();
                            D_ID = district_dist.Sample();
                        }
                    }
                    else
                    {
                        W_ID = wh_dist.Sample();
                        D_ID = district_dist.Sample();
                    }
                    var C_ID = NURand(1023, 1, Constants.NUM_C_PER_D, 0) - 1;
                    var firstGrainID = W_ID * Constants.NUM_D_PER_W + D_ID;
                    var grains = new HashSet<Tuple<int, string>>();
                    grains.Add(new Tuple<int, string>(W_ID, "TPCC.Grains.ItemGrain"));
                    grains.Add(new Tuple<int, string>(W_ID, "TPCC.Grains.WarehouseGrain"));
                    grains.Add(new Tuple<int, string>(firstGrainID, "TPCC.Grains.CustomerGrain"));
                    grains.Add(new Tuple<int, string>(W_ID * Constants.NUM_D_PER_W + D_ID, "TPCC.Grains.DistrictGrain"));
                    grains.Add(new Tuple<int, string>(GetOrderGrain(W_ID, D_ID, C_ID), "TPCC.Grains.OrderGrain"));
                    var ol_cnt = ol_cnt_dist_uni.Sample();
                    var rbk = rbk_dist_uni.Sample();
                    //rbk = 0;
                    var itemsToBuy = new Dictionary<int, Tuple<int, int>>();  // <I_ID, <supply_warehouse, quantity>>

                    var remote_flag = false;

                    for (int i = 0; i < ol_cnt; i++)
                    {
                        int I_ID;

                        if (i == ol_cnt - 1 && rbk == 1) I_ID = -1;
                        else
                        {
                            do I_ID = NURand(8191, 1, Constants.NUM_I, 0) - 1;
                            while (itemsToBuy.ContainsKey(I_ID));
                        }

                        int supply_wh;
                        var local = local_dist_uni.Sample() > 1;
                        if (config.numWarehouse == 1 || local) supply_wh = W_ID;    // supply by home warehouse
                        else   // supply by remote warehouse
                        {
                            remote_flag = true;
                            do supply_wh = wh_dist.Sample();
                            while (supply_wh == W_ID);
                        }
                        var quantity = quantity_dist_uni.Sample();
                        itemsToBuy.Add(I_ID, new Tuple<int, int>(supply_wh, quantity));

                        if (I_ID != -1)
                        {
                            var grainID = GetStockGrain(supply_wh, I_ID);
                            var id = new Tuple<int, string>(grainID, "TPCC.Grains.StockGrain");
                            if (!grains.Contains(id)) grains.Add(id);
                        }
                    }
                    if (remote_flag) remote_count++;
                    txn_size.Add(grains.Count);
                    var req = new RequestData(firstGrainID, C_ID, itemsToBuy);
                    req.grains_in_namespace = grains;
                    shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), req));
                }
            }
            var numTxn = Constants.BASE_NUM_NEWORDER * numRound;
            Console.WriteLine($"siloCPU = {siloCPU}, epoch = {epoch}, remote wh rate = {remote_count * 100.0 / numTxn}%, txn_size_ave = {txn_size.Average()}");
        }

        private static void InitializeTPCCWorkload()
        {
            switch (config.distribution)
            {
                case Distribution.UNIFORM:
                    Console.WriteLine($"Generate UNIFORM data for TPCC");
                    for (int epoch = 0; epoch < config.numEpochs; epoch++) GenerateNewOrder(epoch);
                    break;
                default:
                    throw new Exception($"Exception: TPCC does not support distribution {config.distribution}. ");
            }
        }

        private static void InitializeGetBalanceWorkload()
        {
            var numTxnPerEpoch = Constants.BASE_NUM_MULTITRANSFER * 4 * siloCPU / 4;
            if (config.grainImplementationType == ImplementationType.ORLEANSEVENTUAL) numTxnPerEpoch *= 2;
            var numGrain = config.numAccounts / config.numAccountsPerGroup;
            switch (config.distribution)
            {
                case Distribution.UNIFORM:
                    var dist = new DiscreteUniform(0, numGrain - 1, new Random());
                    for (int epoch = 0; epoch < config.numEpochs; epoch++)
                    {
                        for (int txn = 0; txn < numTxnPerEpoch; txn++)
                        {
                            var grainsPerTxn = new List<int>();
                            grainsPerTxn.Add(dist.Sample());
                            shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(grainsPerTxn)));
                        }
                    }
                    break;
                default:
                    throw new Exception("Exception: NewProcess only support Uniform for GetBalance. ");
            }
        }

        private static void InitializeSmallBankWorkload()
        {
            if (config.mixture[0] == 100)
            {
                InitializeGetBalanceWorkload();
                return;
            }
            if (config.mixture.Sum() > 0) throw new Exception("Exception: NewProcess only support MultiTransfer for SmallBankBenchmark");
            var numTxnPerEpoch = Constants.BASE_NUM_MULTITRANSFER * 10 * siloCPU / 4;   // changed!!
            if (config.grainImplementationType == ImplementationType.ORLEANSEVENTUAL) numTxnPerEpoch *= 2;
            var numGrain = config.numAccounts / config.numAccountsPerGroup;
            var numGrainPerTxn = config.numGrainsMultiTransfer;
            switch (config.distribution)
            {
                case Distribution.UNIFORM:
                    Console.WriteLine($"Generate UNIFORM data for SmallBank..");
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
                            shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(grainsPerTxn)));
                        }
                    }
                    break;
                case Distribution.HOTRECORD:
                    int numHotGrain = (int)(Constants.skewness * numGrain);
                    var numHotGrainPerTxn = Constants.hotRatio * numGrainPerTxn;
                    Console.WriteLine($"Generate data for HOTRECORD, {numHotGrain} hot grains, {numHotGrainPerTxn} hot grain per txn...");
                    var normal_dist = new DiscreteUniform(numHotGrain, numGrain - 1, new Random());
                    DiscreteUniform hot_dist = null;
                    if (numHotGrain > 0) hot_dist = new DiscreteUniform(0, numHotGrain - 1, new Random());
                    for (int epoch = 0; epoch < config.numEpochs; epoch++)
                    {
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
                            shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(grainsPerTxn)));
                        }
                    }
                    break;
                case Distribution.ZIPFIAN:    // read data from file
                    var zipf = config.zipfianConstant;
                    var txnSize = config.numGrainsMultiTransfer;
                    Console.WriteLine($"read data from files, txnsize = {numGrainPerTxn}, zipf = {zipf}");
                    var prefix = Constants.dataPath + $@"MultiTransfer\{numGrainPerTxn}\zipf{zipf}_";

                    // read data from files
                    for (int epoch = 0; epoch < config.numEpochs; epoch++)
                    {
                        string line;
                        var path = prefix + $@"epoch{epoch}.txt";
                        var file = new StreamReader(path);
                        while ((line = file.ReadLine()) != null)
                        {
                            var grainsPerTxn = new List<int>();
                            for (int i = 0; i < txnSize; i++)
                            {
                                if (i > 0) line = file.ReadLine();  // the 0th line has been read by while() loop
                                var id = int.Parse(line);
                                grainsPerTxn.Add(id);
                            }
                            shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(grainsPerTxn)));
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

            thread_requests = new Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>>();
            for (int epoch = 0; epoch < config.numEpochs; epoch++)
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
            int aggNumNotSureSerializable = results[0].numNotSerializable;
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
                aggNumNotSureSerializable += results[i].numNotSureSerializable;
                aggNumDeadlock += results[i].numDeadlock;
                aggStartTime = (results[i].startTime < aggStartTime) ? results[i].startTime : aggStartTime;
                aggEndTime = (results[i].endTime < aggEndTime) ? results[i].endTime : aggEndTime;
                aggLatencies.AddRange(results[i].latencies);
                aggDetLatencies.AddRange(results[i].det_latencies);
            }
            var res = new WorkloadResults(aggNumDetTransactions, aggNumNonDetTransactions, aggNumDetCommitted, aggNumNonDetCommitted, aggStartTime, aggEndTime, aggNumNotSerializable, aggNumNotSureSerializable, aggNumDeadlock);
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
            Console.WriteLine($"detPipe per thread = {detPipeSize}, nonDetPipe per thread = {nonDetPipeSize}");

            ProcessWork();
            //Console.ReadLine();
        }
    }
}