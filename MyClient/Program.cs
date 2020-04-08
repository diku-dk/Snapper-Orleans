using System;
using System.Threading;
using Utilities;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using ExperimentProcess;
using System.Diagnostics;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using SmallBank.Interfaces;

namespace MyClient
{
    class Program
    {
        static int global_tid = 300;
        static int numTxn = 20000;
        static int numClient = 1;
        static int numThread = 1;
        static int numEpoch = 4;
        static Boolean LocalCluster = false;
        static IClusterClient[] clients;
        static Thread[] threads;
        static IBenchmark[] benchmarks;
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
            config.grainImplementationType = ImplementationType.ORLEANSEVENTUAL;
            config.mixture = new int[5];
            config.mixture[0] = 0;
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
            benchmarks = new SmallBankBenchmark[numThread];
            for (int i = 0; i < numThread; i++)
            {
                benchmarks[i] = new SmallBankBenchmark();
                benchmarks[i].generateBenchmark(config);
            }

            // initialize clients
            clients = new IClusterClient[numClient];
            ClientConfiguration clientConfig = new ClientConfiguration();
            for (int i = 0; i < numClient; i++)
            {
                if (LocalCluster) clients[i] = await clientConfig.StartClientWithRetries();
                else clients[i] = await clientConfig.StartClientWithRetriesToCluster();
            }

            /*
            // initialize configuration grain
            Console.WriteLine($"Initializing configuration grain...");
            var nonDetCCType = ConcurrencyType.S2PL;
            var maxNonDetWaitingLatencyInMSecs = 1000;
            var batchIntervalMSecs = 100;
            var backoffIntervalMsecs = 10000;
            var idleIntervalTillBackOffSecs = 120;
            var numCoordinators = (uint)5;
            var exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), maxNonDetWaitingLatencyInMSecs);
            var coordConfig = new CoordinatorGrainConfiguration(batchIntervalMSecs, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numCoordinators);
            var configGrain = clients[0].GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
            await configGrain.UpdateNewConfiguration(exeConfig);
            await configGrain.UpdateNewConfiguration(coordConfig);*/

            // load grains
            Console.WriteLine($"Loading grains...");
            var tasks = new List<Task<FunctionResult>>();
            for (uint i = 0; i < config.numAccounts / config.numAccountsPerGroup; i++)
            {
                var args = new Tuple<uint, uint>(config.numAccountsPerGroup, i);
                var input = new FunctionInput(args);
                var groupGUID = Helper.convertUInt32ToGuid(i);
                //var grain = clients[0].GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                var grain = clients[0].GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(groupGUID);
                tasks.Add(grain.StartTransaction("InitBankAccounts", input));
            }
            await Task.WhenAll(tasks);

            Console.WriteLine($"Start thread work...");
            await ThreadWorkAsync(0);

            Console.WriteLine("Finished running experiment. Press Enter to exit");
            Console.ReadLine();
            return 0;
        }

        private static async Task ThreadWorkAsync(Object obj)
        {
            var globalWatch = new Stopwatch();
            int threadIndex = (int)obj;
            var benchmark = benchmarks[threadIndex];
            var client = clients[threadIndex % numClient];
            for (int eIndex = 0; eIndex < numEpoch; eIndex++)
            {
                Console.WriteLine($"Thread {threadIndex} starts epoch {eIndex}. ");
                var tasks = new List<Task<FunctionResult>>();
                globalWatch.Restart();
                for (int i = 0; i < numTxn; i++) tasks.Add(benchmark.newTransaction(client, global_tid ++));
                await Task.WhenAll(tasks);

                var time = globalWatch.Elapsed;
                int numAbort = 0;
                for (int i = 0; i < numTxn; i++) if (tasks[i].Result.hasException()) numAbort++;
                Console.WriteLine($"Throughput = {1000 * numTxn / time.TotalMilliseconds} per Second, abort rate = {100 * numAbort / numTxn}%");
            }
        }
    }
}
