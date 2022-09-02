using MathNet.Numerics.Statistics;
using Orleans;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Utilities;
using TPCC.Interfaces;
using Orleans.Transactions;

namespace SnapperExperimentProcess
{
    using SharedRequest = Dictionary<int, Queue<Tuple<bool, RequestData>>>;                // <epoch ID, queue>
    using ThreadRequest = Dictionary<int, Dictionary<int, ConcurrentQueue<RequestData>>>;  // <epoch ID, thread ID, queue>

    public class SingleExperimentManager
    {
        readonly int experimentID;
        readonly SiloConfiguration siloConfig;
        readonly Workload workload;
        SharedRequest shared_requests;
        ThreadRequest thread_requests;

        static Barrier[] barriers;
        static CountdownEvent[] threadAcks;
        
        bool[] isEpochFinish;
        bool[] isProducerFinish;
        int detBufferSize;
        int nonDetBufferSize;
        IBenchmark[] benchmarks;

        int numProducer = 1;
        int numDetConsumer;
        int numNonDetConsumer;

        IClusterClient[] clients;
        bool resetDone = false;

        public SingleExperimentManager(int experimentID, SiloConfiguration siloConfig, Workload workload, SharedRequest shared_requests)
        {
            this.experimentID = experimentID;
            this.siloConfig = siloConfig;
            this.workload = workload;
            this.shared_requests = shared_requests;
            numDetConsumer = siloConfig.numCPUPerSilo / Constants.numCPUBasic;
            numNonDetConsumer = siloConfig.numCPUPerSilo / Constants.numCPUBasic;
        }

        public async Task RunOneExperiment()
        {
            if (workload.pactPercent == 100) numNonDetConsumer = 0;
            else if (workload.pactPercent == 0) numDetConsumer = 0;

            switch (siloConfig.benchmarkType)
            {
                case BenchmarkType.SMALLBANK:
                    benchmarks = new SmallBankBenchmark[numDetConsumer + numNonDetConsumer];
                    for (int k = 0; k < numDetConsumer + numNonDetConsumer; k++) benchmarks[k] = new SmallBankBenchmark();
                    break;
                case BenchmarkType.NEWSMALLBANK:
                    benchmarks = new SmallBankBenchmark[numDetConsumer + numNonDetConsumer];
                    for (int k = 0; k < numDetConsumer + numNonDetConsumer; k++) benchmarks[k] = new SmallBankBenchmark();
                    break;
                case BenchmarkType.TPCC:
                    benchmarks = new TPCCBenchmark[numDetConsumer + numNonDetConsumer];
                    for (int k = 0; k < numDetConsumer + numNonDetConsumer; k++) benchmarks[k] = new TPCCBenchmark();
                    break;
                default:
                    throw new Exception($"Exception: unsupported benchmark {siloConfig.benchmarkType}");
            }

            for (int k = 0; k < numDetConsumer + numNonDetConsumer; k++)
            {
                if (k < numDetConsumer) benchmarks[k].GenerateBenchmark(siloConfig.implementationType, true, workload.noDeadlock);
                else benchmarks[k].GenerateBenchmark(siloConfig.implementationType, false, workload.noDeadlock);
            }

            // some initialization for generating transactions
            if (detBufferSize == 0 && nonDetBufferSize == 0)
            {
                if (numDetConsumer > 0) detBufferSize = workload.pactPipeSize * 10;
                if (numNonDetConsumer > 0) nonDetBufferSize = workload.actPipeSize * 10;
            }
            Console.WriteLine($"Run experiment: pactPercent = {workload.pactPercent}%, txnSize = {workload.txnSize}, {workload.distribution} distribution, zipf = {workload.zipfianConstant}, pactPipeSize = {workload.pactPipeSize}, actPipeSize = {workload.actPipeSize}");

            InitializeProducerThread();
            await InitializeClients();

            var results = new WorkloadResult[numDetConsumer + numNonDetConsumer];
            InitializeConsumerThreads(results);

            var aggResults = new WorkloadResult[Constants.numEpoch];
            for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
            {
                barriers[epoch].SignalAndWait();
                threadAcks[epoch].Wait();
                aggResults[epoch] = AggregateResults(results);

                if (siloConfig.benchmarkType == BenchmarkType.TPCC) 
                {
                    ResetOrderGrain();
                    while(!resetDone) Thread.Sleep(100);
                    resetDone = false;
                }
            }

            if (siloConfig.benchmarkType == BenchmarkType.NEWSMALLBANK)
            {
                // print result header in the breakdown_latency.txt file
                using (var file = new StreamWriter(Constants.latencyPath, true))
                {
                    file.WriteLine("Fig    Silo_vCPU   implementation   txnSize numWriter   distribution    latency_50th_ms    breakdown_latency_I1_I9");
                }
            }

            PrintResults(aggResults);
        }

        async void ResetOrderGrain()
        {
            var index = 0;
            var tpccManager = new TPCCManager();
            tpccManager.Init(siloConfig.numCPUPerSilo, siloConfig.NUM_OrderGrain_PER_D);
            var NUM_W_PER_SILO = Helper.GetNumWarehousePerSilo(siloConfig.numCPUPerSilo);
            var tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    for (int i = 0; i < tpccManager.NUM_OrderGrain_PER_D; i++)
                    {
                        index = W_ID * tpccManager.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * tpccManager.NUM_OrderGrain_PER_D + i;
                        var input = new Tuple<int, int, int>(W_ID, D_ID, i);
                        if (siloConfig.implementationType == ImplementationType.NONTXN)
                        {
                            var grain = clients[0].GetGrain<IEventualOrderGrain>(index);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        else
                        {
                            var grain = clients[0].GetGrain<IOrderGrain>(index);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        if (tasks.Count == Environment.ProcessorCount)
                        {
                            await Task.WhenAll(tasks);
                            tasks.Clear();
                        }
                    }
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish re-setting OrderGrains. ");
            resetDone = true;
        }


        void InitializeProducerThread()
        {
            barriers = new Barrier[Constants.numEpoch];
            threadAcks = new CountdownEvent[Constants.numEpoch];
            for (int i = 0; i < Constants.numEpoch; i++)
            {
                barriers[i] = new Barrier(numDetConsumer + numNonDetConsumer + 1);
                threadAcks[i] = new CountdownEvent(numDetConsumer + numNonDetConsumer);
            }

            thread_requests = new ThreadRequest();
            for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
            {
                thread_requests.Add(epoch, new Dictionary<int, ConcurrentQueue<RequestData>>());
                for (int t = 0; t < numDetConsumer + numNonDetConsumer; t++) thread_requests[epoch].Add(t, new ConcurrentQueue<RequestData>());
            }
            for (int producer = 0; producer < numProducer; producer++)
            {
                var thread = new Thread(ProducerThreadWork);
                thread.Start();
            }
        }

        async Task InitializeClients()
        {
            clients = new IClusterClient[numDetConsumer + numNonDetConsumer];
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                var orleansClientManager = new OrleansClientManager(siloConfig);
                clients[i] = await orleansClientManager.StartClientWithRetries();
            }   
        }

        void InitializeConsumerThreads(WorkloadResult[] results)
        {
            for (int i = 0; i < numDetConsumer + numNonDetConsumer; i++)
            {
                var thread = new Thread(ThreadWorkAsync);
                if (i < numDetConsumer) thread.Start(new Tuple<int, bool, WorkloadResult[]>(i, true, results));
                else thread.Start(new Tuple<int, bool, WorkloadResult[]>(i, false, results));
            }
        }

        public void ProducerThreadWork(object _)
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

        public async void ThreadWorkAsync(object obj)
        {
            var input = (Tuple<int, bool, WorkloadResult[]>)obj;
            var threadIndex = input.Item1;
            var isDet = input.Item2;
            var results = input.Item3;
            var pipeSize = isDet ? workload.pactPipeSize : workload.actPipeSize;
            var globalWatch = new Stopwatch();
            var benchmark = benchmarks[threadIndex];
            var client = clients[threadIndex % (numDetConsumer + numNonDetConsumer)];
            for (int eIndex = 0; eIndex < Constants.numEpoch; eIndex++)
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
                var submitTxnTime = new Dictionary<Task<TransactionResult>, DateTime>();
                var breakdown = new List<double>[Constants.numIntervals];
                for (int i = 0; i < Constants.numIntervals; i++) breakdown[i] = new List<double>();
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
                        var now = DateTime.Now;
                        var asyncReqStartTime = globalWatch.Elapsed;
                        Task<TransactionResult> newTask;
                        if (siloConfig.benchmarkType == BenchmarkType.NEWSMALLBANK)
                            newTask = benchmark.NewTransactionWithNOOP(client, txn, workload.numWriter);
                        else newTask = benchmark.NewTransaction(client, txn);
                        numEmit++;

                        reqs.Add(newTask, asyncReqStartTime);
                        tasks.Add(newTask);
                        submitTxnTime.Add(newTask, now);
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
                            Debug.Assert(siloConfig.implementationType == ImplementationType.ORLEANSTXN);
                            //Console.WriteLine($"Exception:{e.Message}, {e.StackTrace}");
                            noException = false;
                        }
                        numOrleansTxnEmit++;
                        if (noException)
                        {
                            var latency = (asyncReqEndTime - reqs[task]).TotalMilliseconds;
                            if (isDet)   // for det
                            {
                                if (siloConfig.benchmarkType == BenchmarkType.SMALLBANK) Debug.Assert(!task.Result.exception);
                                numDetCommit++;
                                det_latencies.Add(latency);
                            }
                            else    // for non-det + eventual + orleans txn
                            {
                                numNonDetTransaction++;
                                if (!task.Result.exception)
                                {
                                    numNonDetCommit++;
                                    latencies.Add(latency);

                                    if (siloConfig.benchmarkType == BenchmarkType.NEWSMALLBANK)
                                    {
                                        var r = task.Result;
                                        var time = new double[Constants.numIntervals];
                                        time[0] = (r.beforeGetTidTime - submitTxnTime[task]).TotalMilliseconds;
                                        time[1] = (r.afterGetTidTime - r.beforeGetTidTime).TotalMilliseconds;
                                        time[2] = (r.beforeExeTime - r.afterGetTidTime).TotalMilliseconds;
                                        time[3] = (r.beforeUpdate1Time - r.beforeExeTime).TotalMilliseconds;
                                        time[4] = (r.callGrainTime - r.beforeUpdate1Time).TotalMilliseconds;
                                        time[5] = (r.afterExeTime - r.callGrainTime).TotalMilliseconds;
                                        time[6] = (r.beforeResolveTime - r.afterExeTime).TotalMilliseconds;
                                        time[7] = (r.afterResolveTime - r.beforeResolveTime).TotalMilliseconds;
                                        time[8] = latency;
                                        for (int i = 0; i < Constants.numIntervals - 1; i++) 
                                            time[Constants.numIntervals - 1] -= time[i];
                                        for (int i = 0; i < Constants.numIntervals; i++) 
                                            breakdown[i].Add(time[i]);
                                    }
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
                while (globalWatch.ElapsedMilliseconds < Constants.epochDurationMSecs && (queue.Count != 0 || !isProducerFinish[eIndex]));
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
                        Debug.Assert(siloConfig.implementationType == ImplementationType.ORLEANSTXN);
                        //Console.WriteLine($"Exception:{e.Message}, {e.StackTrace}");
                        noException = false;
                    }
                    numOrleansTxnEmit++;
                    if (noException)
                    {
                        var latency = (asyncReqEndTime - reqs[task]).TotalMilliseconds;
                        if (isDet)   // for det
                        {
                            if (siloConfig.benchmarkType == BenchmarkType.SMALLBANK) Debug.Assert(!task.Result.exception);
                            numDetCommit++;
                            det_latencies.Add(latency);
                        }
                        else    // for non-det + eventual + orleans txn
                        {
                            numNonDetTransaction++;
                            if (!task.Result.exception)
                            {
                                numNonDetCommit++;
                                latencies.Add(latency);

                                if (siloConfig.benchmarkType == BenchmarkType.NEWSMALLBANK)
                                {
                                    var r = task.Result;
                                    var time = new double[Constants.numIntervals];
                                    time[0] = (r.beforeGetTidTime - submitTxnTime[task]).TotalMilliseconds;
                                    time[1] = (r.afterGetTidTime - r.beforeGetTidTime).TotalMilliseconds;
                                    time[2] = (r.beforeExeTime - r.afterGetTidTime).TotalMilliseconds;
                                    time[3] = (r.beforeUpdate1Time - r.beforeExeTime).TotalMilliseconds;
                                    time[4] = (r.callGrainTime - r.beforeUpdate1Time).TotalMilliseconds;
                                    time[5] = (r.afterExeTime - r.callGrainTime).TotalMilliseconds;
                                    time[6] = (r.beforeResolveTime - r.afterExeTime).TotalMilliseconds;
                                    time[7] = (r.afterResolveTime - r.beforeResolveTime).TotalMilliseconds;
                                    time[8] = latency;
                                    for (int i = 0; i < Constants.numIntervals - 1; i++)
                                        time[Constants.numIntervals - 1] -= time[i];
                                    for (int i = 0; i < Constants.numIntervals; i++)
                                        breakdown[i].Add(time[i]);
                                }
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
                if (isDet) Console.WriteLine($"det-commit = {numDetCommit}, tp = {1000 * numDetCommit / (endTime - startTime)}. ");
                else
                {
                    if (siloConfig.implementationType == ImplementationType.ORLEANSTXN) Console.WriteLine($"total_num_nondet = {numOrleansTxnEmit}, nondet-commit = {numNonDetCommit}, tp = {1000 * numNonDetCommit / (endTime - startTime)}, Deadlock = {numDeadlock}, NotSerilizable = {numNotSerializable}, NotSureSerializable = {numNotSureSerializable}");
                    else Console.WriteLine($"total_num_nondet = {numNonDetTransaction}, nondet-commit = {numNonDetCommit}, tp = {1000 * numNonDetCommit / (endTime - startTime)}, Deadlock = {numDeadlock}, NotSerilizable = {numNotSerializable}, NotSureSerializable = {numNotSureSerializable}");
                }
                WorkloadResult res;
                if (siloConfig.implementationType == ImplementationType.ORLEANSTXN) res = new WorkloadResult(numDetCommit, numOrleansTxnEmit, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numNotSureSerializable, numDeadlock);
                else res = new WorkloadResult(numDetCommit, numNonDetTransaction, numDetCommit, numNonDetCommit, startTime, endTime, numNotSerializable, numNotSureSerializable, numDeadlock);
                res.setLatency(latencies, det_latencies);
                res.setBreakdownLatency(breakdown);
                results[threadIndex] = res;
                threadAcks[eIndex].Signal();  // Signal the completion of epoch
            }
        }

        static WorkloadResult AggregateResults(WorkloadResult[] results)
        {
            Debug.Assert(results.Length >= 1);
            var aggNumDetCommitted = results[0].numDetCommitted;
            var aggNumNonDetCommitted = results[0].numNonDetCommitted;
            var aggNumDetTransactions = results[0].numDetTxn;
            var aggNumNonDetTransactions = results[0].numNonDetTxn;
            var aggNumNotSerializable = results[0].numNotSerializable;
            var aggNumNotSureSerializable = results[0].numNotSerializable;
            var aggNumDeadlock = results[0].numDeadlock;
            var aggStartTime = results[0].startTime;
            var aggEndTime = results[0].endTime;
            var aggLatencies = new List<double>();
            var aggDetLatencies = new List<double>();
            aggLatencies.AddRange(results[0].latencies);
            aggDetLatencies.AddRange(results[0].det_latencies);

            var aggBreakdown = new List<double>[Constants.numIntervals];
            for (int i = 0; i < Constants.numIntervals; i++)
            {
                aggBreakdown[i] = new List<double>();
                aggBreakdown[i].AddRange(results[0].breakdown[i]);
            }

            for (int i = 1; i < results.Length; i++)    // each thread has a result
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
            var res = new WorkloadResult(aggNumDetTransactions, aggNumNonDetTransactions, aggNumDetCommitted, aggNumNonDetCommitted, aggStartTime, aggEndTime, aggNumNotSerializable, aggNumNotSureSerializable, aggNumDeadlock);
            res.setLatency(aggLatencies, aggDetLatencies);
            res.setBreakdownLatency(aggBreakdown);
            return res;
        }

        static string ChangeFormat(double n)
        {
            return Math.Round(n, 4).ToString().Replace(',', '.');
        }

        void PrintResults(WorkloadResult[] aggResults)
        {
            var aggLatencies = new List<double>();
            var aggDetLatencies = new List<double>();
            var detThroughPutAccumulator = new List<float>();
            var nonDetThroughPutAccumulator = new List<float>();
            var abortRateAccumulator = new List<double>();
            var notSerializableRateAccumulator = new List<float>();
            var notSureSerializableRateAccumulator = new List<float>();
            var deadlockRateAccumulator = new List<float>();

            var aggBreakdown = new List<double>[Constants.numIntervals];
            for (int i = 0; i < Constants.numIntervals; i++) aggBreakdown[i] = new List<double>();

            // Skip the epochs upto warm up epochs
            for (int epoch = Constants.numWarmupEpoch; epoch < Constants.numEpoch; epoch++)
            {
                var aggResult = aggResults[epoch];
                aggLatencies.AddRange(aggResult.latencies);
                aggDetLatencies.AddRange(aggResult.det_latencies);

                for (int i = 0; i < Constants.numIntervals; i++) aggBreakdown[i].AddRange(aggResult.breakdown[i]);

                var time = aggResult.endTime - aggResult.startTime;
                float detCommittedTxnThroughput = (float)aggResult.numDetCommitted * 1000 / time;  // the throughput only include committed transactions
                float nonDetCommittedTxnThroughput = (float)aggResult.numNonDetCommitted * 1000 / time;
                var numAbort = aggResult.numNonDetTxn - aggResult.numNonDetCommitted;
                var abortRate = numAbort * 100.0 / aggResult.numNonDetTxn;    // the abort rate is based on all non-det txns
                if (numAbort > 0)
                {
                    var notSerializable = aggResult.numNotSerializable * 100.0 / numAbort;   // number of transactions abort due to not serializable among all aborted transactions
                    notSerializableRateAccumulator.Add((float)notSerializable);
                    var notSureSerializable = aggResult.numNotSureSerializable * 100.0 / numAbort;   // abort due to incomplete AfterSet
                    notSureSerializableRateAccumulator.Add((float)notSureSerializable);
                    var deadlock = aggResult.numDeadlock * 100.0 / numAbort;
                    deadlockRateAccumulator.Add((float)deadlock);
                }
                detThroughPutAccumulator.Add(detCommittedTxnThroughput);
                nonDetThroughPutAccumulator.Add(nonDetCommittedTxnThroughput);
                abortRateAccumulator.Add(abortRate);
            }

            //Compute statistics on the accumulators, maybe a better way is to maintain a sorted list
            var detThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(detThroughPutAccumulator.ToArray());
            var nonDetThroughputMeanAndSd = ArrayStatistics.MeanStandardDeviation(nonDetThroughPutAccumulator.ToArray());
            var abortRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(abortRateAccumulator.ToArray());
            var notSerializableRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(notSerializableRateAccumulator.ToArray());
            var notSureSerializableRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(notSureSerializableRateAccumulator.ToArray());
            var deadlockRateMeanAndSd = ArrayStatistics.MeanStandardDeviation(deadlockRateAccumulator.ToArray());
            

            if (siloConfig.benchmarkType == BenchmarkType.NEWSMALLBANK)
            {
                using (var file = new StreamWriter(Constants.latencyPath, true))
                {
                    file.Write($"Fig.{experimentID} {siloConfig.numCPUPerSilo}  {siloConfig.implementationType} {workload.txnSize}  {workload.numWriter}    {workload.distribution} ");

                    var lat = ArrayStatistics.PercentileInplace(aggLatencies.ToArray(), 50);    // 50th total latency
                    file.Write($"{ChangeFormat(lat)}");

                    file.WriteLine();

                    var num = aggBreakdown[0].Count;
                    for (int i = 0; i < Constants.numIntervals; i++) Debug.Assert(aggBreakdown[i].Count == num);

                    for (int j = 0; j < num; j++)
                    {
                        for (int i = 0; i < Constants.numIntervals; i++) file.Write($"{ChangeFormat(aggBreakdown[i][j])}    ");    // need to filter out weird data points
                        file.WriteLine();
                    }
                }
                return;
            }

            using (var file = new StreamWriter(Constants.resultPath, true))
            {
                file.Write($"Fig.{experimentID} ");
                file.Write($"{siloConfig.numCPUPerSilo}  {siloConfig.implementationType} {siloConfig.benchmarkType}  {siloConfig.loggingEnabled}    {siloConfig.NUM_OrderGrain_PER_D}  ");
                file.Write($"{workload.pactPercent}% {workload.txnSize}  {workload.numWriter}    {workload.distribution} {workload.zipfianConstant}  {workload.actPipeSize}  {workload.pactPipeSize} {workload.noDeadlock}   ");

                file.Write($"{detThroughputMeanAndSd.Item1:0}   {detThroughputMeanAndSd.Item2:0}    ");
                file.Write($"{nonDetThroughputMeanAndSd.Item1:0}    {nonDetThroughputMeanAndSd.Item2:0} {ChangeFormat(abortRateMeanAndSd.Item1)}%   ");

                var abortRWConflict = 100 - deadlockRateMeanAndSd.Item1 - notSerializableRateMeanAndSd.Item1 - notSureSerializableRateMeanAndSd.Item1;
                file.Write($"{ChangeFormat(abortRWConflict)}%   ");
                file.Write($"{ChangeFormat(deadlockRateMeanAndSd.Item1)}%   ");
                file.Write($"{ChangeFormat(notSureSerializableRateMeanAndSd.Item1)}%    ");
                file.Write($"{ChangeFormat(notSerializableRateMeanAndSd.Item1)}%    ");

                foreach (var percentile in Constants.percentilesToCalculate)
                {
                    var lat = ArrayStatistics.PercentileInplace(aggDetLatencies.ToArray(), percentile);
                    file.Write($"{ChangeFormat(lat)}    ");
                }

                foreach (var percentile in Constants.percentilesToCalculate)
                {
                    var lat = ArrayStatistics.PercentileInplace(aggLatencies.ToArray(), percentile);
                    file.Write($"{ChangeFormat(lat)}    ");
                }

                file.WriteLine();
            }
        }
    }
}
