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
        static int numTxn = 5000;
        static int numClient = 1;
        static int numThread = 1;
        static int numEpoch = 6;
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
            config.grainImplementationType = ImplementationType.SNAPPER;
            config.mixture = new int[5];
            config.mixture[0] = 100;
            config.mixture[1] = 0;
            config.mixture[2] = 0;
            config.mixture[3] = 0;
            config.mixture[4] = 0;
            config.numAccounts = 1000000;
            config.numAccountsMultiTransfer = 0;
            config.numAccountsPerGroup = 100;
            config.numGrainsMultiTransfer = 0;
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
            await configGrain.UpdateNewConfiguration(coordConfig);
            Console.WriteLine($"Finish initializing configuration grain.");

            // load grains
            var tasks = new List<Task<FunctionResult>>();
            var batchSize = -1; //If you want to load the grains in sequence instead of all concurrent
            for (uint i = 0; i < config.numAccounts / config.numAccountsPerGroup; i++)
            {
                var args = new Tuple<uint, uint>(config.numAccountsPerGroup, i);
                var input = new FunctionInput(args);
                var groupGUID = Helper.convertUInt32ToGuid(i);
                var sntxnGrain = clients[0].GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                tasks.Add(sntxnGrain.StartTransaction("InitBankAccounts", input));
                if (batchSize > 0 && (i + 1) % batchSize == 0)
                {
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            if (tasks.Count > 0)
            {
                await Task.WhenAll(tasks);
            }
            Console.WriteLine($"Finish loading grains. ");*/

            // initialize threads
            /*
            threads = new Thread[numThread];
            for (int i = 0; i < numThread; i++)
            {
                Thread thread = new Thread(ThreadWorkAsync);
                threads[i] = thread;
            }

            for (int i = 0; i < numThread; i++)
            {
                threads[i].Start(i);
                threads[i].Join();
            } */
            var tasks = new List<Task>();
            for (int i = 0; i < numThread; i++)
            {
                tasks.Add(ThreadWorkAsync(i));
            }
            await Task.WhenAll(tasks);
            //await ThreadWorkAsync(0);

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
                //globalWatch.Restart();
                for (int i = 0; i < numTxn; i++) tasks.Add(benchmark.newTransaction(client));
                globalWatch.Restart();
                await Task.WhenAll(tasks);

                var time = globalWatch.Elapsed;
                int numAbort = 0;
                for (int i = 0; i < numTxn; i++)
                {
                    if (tasks[i].Result.hasException()) numAbort++;
                }

                Console.WriteLine($"Throughput = {1000 * numTxn / time.TotalMilliseconds} per Second, abort rate = {100 * numAbort / numTxn}%");
            }
        }
    }
}
