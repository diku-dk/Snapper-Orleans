using System;
using Utilities;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using ExperimentProcess;
using System.Diagnostics;
using SmallBank.Interfaces;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using System.Threading;

namespace MyClient
{
    class Program
    {
        static int global_tid = 0;
        static int numTxn = 10000;
        static int numThread = 2;
        static int numEpoch = 1;
        static Boolean LocalCluster = true;
        static IClusterClient client;
        static IBenchmark benchmark;
        static WorkloadConfiguration config;

        static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            // initialize configuration
            config = new WorkloadConfiguration();
            config.benchmark = BenchmarkType.SMALLBANK;
            config.deterministicTxnPercent = 100;
            config.distribution = Distribution.UNIFORM;
            config.grainImplementationType = ImplementationType.SNAPPER;
            config.mixture = new int[5];
            config.mixture[0] = 100;
            config.mixture[1] = 0;
            config.mixture[2] = 0;
            config.mixture[3] = 0;
            config.mixture[4] = 0;
            config.numAccounts = 200;
            config.numAccountsMultiTransfer = 32;
            config.numAccountsPerGroup = 1;
            config.numGrainsMultiTransfer = 4;
            config.zipfianConstant = 0;

            // initialize workload
            benchmark = new SmallBankBenchmark();
            benchmark.generateBenchmark(config);
            
            // initialize clients
            ClientConfiguration clientConfig = new ClientConfiguration();
            if (LocalCluster) client = await clientConfig.StartClientWithRetries();
            else client = await clientConfig.StartClientWithRetriesToCluster();

            // initialize configuration grain
            Console.WriteLine($"Initializing configuration grain...");
            var nonDetCCType = ConcurrencyType.S2PL;
            var maxNonDetWaitingLatencyInMSecs = 1000;
            var batchIntervalMSecs = 100;
            var backoffIntervalMsecs = 10000;
            var idleIntervalTillBackOffSecs = 120;
            var numCoordinators = (uint)2;
            var exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), maxNonDetWaitingLatencyInMSecs);
            var coordConfig = new CoordinatorGrainConfiguration(batchIntervalMSecs, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numCoordinators);
            var configGrain = client.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
            await configGrain.UpdateNewConfiguration(exeConfig);
            await configGrain.UpdateNewConfiguration(coordConfig);

            // load grains
            Console.WriteLine($"Loading grains...");
            var tasks = new List<Task<FunctionResult>>();
            for (uint i = 0; i < config.numAccounts / config.numAccountsPerGroup; i++)
            {
                var args = new Tuple<uint, uint>(config.numAccountsPerGroup, i);
                var input = new FunctionInput(args);
                var groupGUID = Helper.convertUInt32ToGuid(i);
                var grain = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                //var grain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(groupGUID);
                tasks.Add(grain.StartTransaction("InitBankAccounts", input));
            }
            await Task.WhenAll(tasks);

            for (int i = 0; i < numThread; i++)
            {
                var thread = new Thread(ThreadWorkAsync);
                thread.Start(i);
            }
            
            Console.WriteLine("Finished running experiment. Press Enter to exit");
            Console.ReadLine();
            return 0;
        }

        private static async void ThreadWorkAsync(Object obj)
        {
            int threadIndex = (int)obj;
            var globalWatch = new Stopwatch();
            for (int eIndex = 0; eIndex < numEpoch; eIndex++)
            {
                Console.WriteLine($"Thread {threadIndex} starts epoch {eIndex}. ");
                //var t = new List<Task<FunctionResult>>();
                var t = new List<Task<TransactionContext>>();
                globalWatch.Restart();
                var startTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                for (int i = 0; i < numTxn; i++) t.Add(benchmark.newTransaction(client, global_tid++));
                await Task.WhenAll(t);
                long endTime = DateTime.Now.Ticks / TimeSpan.TicksPerMillisecond;
                globalWatch.Stop();
                var time = endTime - startTime;
                /*
                int numAbort = 0;
                for (int i = 0; i < numTxn; i++) if (t[i].Result.hasException()) numAbort++;
                Console.WriteLine($"numTxn = {numTxn}, throughput = {1000 * numTxn / time.TotalMilliseconds} per Second, abort rate = {100 * numAbort / numTxn}%");
                */
                Console.WriteLine($"Thread {threadIndex}, start {startTime.ToString()}, end {endTime.ToString()}, numTxn = {numTxn}, time = {time} ms, throughput = {1000 * numTxn / time} per Second. ");
            }
        }
    }
}
