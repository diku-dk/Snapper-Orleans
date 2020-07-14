using System;
using Utilities;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using ExperimentProcess;
using SmallBank.Interfaces;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using System.Threading;
using System.Linq;
using System.Diagnostics;
using SmallBank.Grains;

namespace TestHybrid
{
    class Program
    {
        static int Max = 3000;
        static int numTxn = 10000;
        static IClusterClient client;
        static Random random = new Random();
        //static uint numGrain = (uint)random.Next(Max);
        static uint numGrain = 500;
        static uint numAccountPerGrain = 1;
        static uint numCoordinator = 2;
        static int num_txn_type = 6;
        static int[] pact_txn_type = new int[num_txn_type];
        static int[] act_txn_type = new int[num_txn_type];
        static int[] act_commit_txn_type = new int[num_txn_type];
        static IBenchmark benchmark = new SmallBankBenchmark();
        static ConcurrencyType nonDetCCType = ConcurrencyType.S2PL;
        //static ConcurrencyType nonDetCCType = ConcurrencyType.TIMESTAMP;
        static volatile bool asyncInitializationDone = false;
        static volatile bool loadingDone = false;
        static WorkloadConfiguration config = new WorkloadConfiguration();
        static ExecutionGrainConfiguration exeConfig;
        static CoordinatorGrainConfiguration coordConfig;
        static Dictionary<Guid, Tuple<float, float>> balances = new Dictionary<Guid, Tuple<float, float>>();

        static int Main(string[] args)
        {
            Console.WriteLine("The test is started. ");

            // initialize transaction type distribution
            var txn_type = new int[num_txn_type];
            for (int i = 0; i < num_txn_type; i++)
            {
                //txn_type[i] = random.Next(Max);
                txn_type[i] = 0;
                pact_txn_type[i] = 0;
                act_txn_type[i] = 0;
                act_commit_txn_type[i] = 0;
            }
            txn_type[2] = 1;

            // initialize benchmark configuration
            config.distribution = Distribution.UNIFORM;
            config.numAccountsPerGroup = numAccountPerGrain;
            config.numAccounts = numAccountPerGrain * numGrain;
            config.deterministicTxnPercent = 0;
            config.mixture = new int[num_txn_type - 1];
            for (int i = 0; i < num_txn_type - 1; i++) config.mixture[i] = txn_type[i] * 100 / txn_type.Sum();
            config.grainImplementationType = ImplementationType.SNAPPER;
            benchmark.generateBenchmark(config);

            // Initialize the client to silo cluster, create configurator grain
            InitiateClientAndSpawnConfigurationCoordinator();
            while (!asyncInitializationDone)
                Thread.Sleep(100);

            // Create the workload grains, load with data
            LoadGrains();
            while (!loadingDone)
                Thread.Sleep(100);

            return DoTransactions().Result;
        }

        private static async Task<int> DoTransactions()
        {
            Console.WriteLine("Emitting transactions ...");
            var tasks = new List<Task<FunctionResult>>();
            for (int i = 0; i < numTxn; i++) tasks.Add(benchmark.newTransaction(client));
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}. ");
                return 0;
            }

            Console.WriteLine("Check the final state of all grains. ");
            // check the final state of all grains
            var ts = new List<Task<FunctionResult>>();
            for (uint i = 0; i < numGrain; i++)
            {
                var groupGUID = Helper.convertUInt32ToGuid(i);
                var grain = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                var accountId = i * numAccountPerGrain;
                var input = new FunctionInput(accountId.ToString());
                ts.Add(grain.StartTransaction(TxnType.Balance.ToString(), input));
            }
            try
            {
                await Task.WhenAll(ts);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}");
            }

            Console.WriteLine("Test correctness of state. ");
            float real_all_saving = 0;
            float real_all_checking = 0;
            foreach (var t in ts)
            {
                Debug.Assert(!t.Result.hasException());
                real_all_saving += ((CustomerAccountGroup)t.Result.afterState.First().Value).savingAccount.First().Value;
                real_all_checking += ((CustomerAccountGroup)t.Result.afterState.First().Value).checkingAccount.First().Value;
            }

            var abort = 0;
            foreach (var t in tasks)
            {
                var type = t.Result.txnType;
                if (t.Result.isDet) Debug.Assert(!t.Result.hasException());
                if (t.Result.hasException()) abort++;
                else
                {
                    Debug.Assert(!t.Result.Exp_2PC && !t.Result.Exp_AppLogic && !t.Result.Exp_NotSerializable && !t.Result.Exp_RWConflict && !t.Result.Exp_UnExpect);
                    if (!t.Result.isDet)
                    {
                        Debug.Assert(t.Result.beforeState.Count == t.Result.afterState.Count);
                        foreach (var item in t.Result.beforeState) Debug.Assert(t.Result.afterState.ContainsKey(item.Key));
                        foreach (var item in t.Result.beforeState)
                        {
                            var grain = item.Key;
                            var before_saving = ((CustomerAccountGroup)item.Value).savingAccount.First().Value;
                            var before_checking = ((CustomerAccountGroup)item.Value).checkingAccount.First().Value;
                            var item2 = t.Result.afterState[grain];
                            var after_saving = ((CustomerAccountGroup)item2).savingAccount.First().Value;
                            var after_checking = ((CustomerAccountGroup)item2).checkingAccount.First().Value;
                            if (nonDetCCType == ConcurrencyType.S2PL) Debug.Assert(before_saving == after_saving && before_checking == after_checking);
                            else if (type != TxnType.Balance.ToString()) Debug.Assert(before_saving == after_saving && before_checking == after_checking);
                        }
                    }
                }

                switch (type)
                {
                    case "Balance":
                        if (t.Result.isDet) pact_txn_type[0]++;
                        else
                        {
                            act_txn_type[0]++;
                            if (!t.Result.hasException()) act_commit_txn_type[0]++;
                        }
                        break;
                    case "DepositChecking":
                        if (t.Result.isDet) pact_txn_type[1]++;
                        else
                        {
                            act_txn_type[1]++;
                            if (!t.Result.hasException()) act_commit_txn_type[1]++;
                        }
                        break;
                    case "Transfer":
                        if (t.Result.isDet) pact_txn_type[2]++;
                        else
                        {
                            act_txn_type[2]++;
                            if (!t.Result.hasException()) act_commit_txn_type[2]++;
                        }
                        break;
                    case "TransactSaving":
                        if (t.Result.isDet) pact_txn_type[3]++;
                        else
                        {
                            act_txn_type[3]++;
                            if (!t.Result.hasException()) act_commit_txn_type[3]++;
                        }
                        break;
                    case "WriteCheck":
                        if (t.Result.isDet) pact_txn_type[4]++;
                        else
                        {
                            act_txn_type[4]++;
                            if (!t.Result.hasException()) act_commit_txn_type[4]++;
                        }
                        break;
                    case "MultiTransfer":
                        if (t.Result.isDet) pact_txn_type[5]++;
                        else
                        {
                            act_txn_type[5]++;
                            if (!t.Result.hasException()) act_commit_txn_type[5]++;
                        }
                        break;
                    default:
                        throw new Exception("Unknown transaction type. ");
                }
            }
            Console.WriteLine($"Total number of txn = {numTxn}, abort = {abort}, abort rate = {100.0 * abort / numTxn}%. ");
            Console.WriteLine($"{TxnType.Balance}           pact = {pact_txn_type[0]}   act = {act_txn_type[0]}   abort = {act_txn_type[0] - act_commit_txn_type[0]}");
            Console.WriteLine($"{TxnType.DepositChecking}   pact = {pact_txn_type[1]}   act = {act_txn_type[1]}   abort = {act_txn_type[1] - act_commit_txn_type[1]}");
            Console.WriteLine($"{TxnType.Transfer}          pact = {pact_txn_type[2]}   act = {act_txn_type[2]}   abort = {act_txn_type[2] - act_commit_txn_type[2]}");
            Console.WriteLine($"{TxnType.TransactSaving}    pact = {pact_txn_type[3]}   act = {act_txn_type[3]}   abort = {act_txn_type[3] - act_commit_txn_type[3]}");
            Console.WriteLine($"{TxnType.WriteCheck}        pact = {pact_txn_type[4]}   act = {act_txn_type[4]}   abort = {act_txn_type[4] - act_commit_txn_type[4]}");
            Console.WriteLine($"{TxnType.MultiTransfer}     pact = {pact_txn_type[5]}   act = {act_txn_type[5]}   abort = {act_txn_type[5] - act_commit_txn_type[5]}");

            // check the total amount of saving and checking balance
            var expect_all_saving = numGrain * 1000 - pact_txn_type[3] - act_commit_txn_type[3];    // TransactSaving
            var expect_all_checking = numGrain * 1000 + pact_txn_type[1] + act_commit_txn_type[1] - pact_txn_type[4] - act_commit_txn_type[4];  // DepositChecking, WriteCheck
            Debug.Assert(expect_all_saving == real_all_saving);
            Debug.Assert(expect_all_checking == real_all_checking);

            Console.WriteLine("Finished running experiment. Press Enter to exit");
            Console.ReadLine();

            return 0;
        }

        private static async void InitiateClientAndSpawnConfigurationCoordinator()
        {
            ClientConfiguration config = new ClientConfiguration();
            client = await config.StartClientWithRetries();

            exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), 1000);
            coordConfig = new CoordinatorGrainConfiguration(100, 10000, 120, numCoordinator);
            var configGrain = client.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
            await configGrain.UpdateNewConfiguration(exeConfig);
            await configGrain.UpdateNewConfiguration(coordConfig);
            Console.WriteLine("Spawned the configuration grain.");
            asyncInitializationDone = true;
        }

        private static async void LoadGrains()
        {
            Console.WriteLine($"Load grains, numGrains = {numGrain}, numAccountPerGroup = {numAccountPerGrain}. ");
            var tasks = new List<Task<FunctionResult>>();
            for (uint i = 0; i < numGrain; i++)
            {
                var groupGUID = Helper.convertUInt32ToGuid(i);
                var args = new Tuple<uint, uint>(numAccountPerGrain, i);
                var input = new FunctionInput(args);
                var sntxnGrain = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                tasks.Add(sntxnGrain.StartTransaction("InitBankAccounts", input));
                balances.Add(groupGUID, new Tuple<float, float>(uint.MaxValue, uint.MaxValue));
            }
            await Task.WhenAll(tasks);
            foreach (var t in tasks) Debug.Assert(!t.Result.hasException());
            Console.WriteLine("Finish loading grains.");
            loadingDone = true;
        }
    }
}
