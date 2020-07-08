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

namespace TestCC
{
    class Program
    {
        static int Max = 3000;
        static int global_tid = 0;
        static IClusterClient client;
        static Random random = new Random();
        //static uint numGrain = (uint)random.Next(Max);
        static uint numGrain = 500;
        static uint numAccountPerGrain = 1;
        static int num_txn_type = 6;
        static int[] txn_type = new int[num_txn_type];
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
            for (int i = 0; i < num_txn_type; i++) txn_type[i] = random.Next(Max);

            // initialize benchmark configuration
            config.distribution = Distribution.UNIFORM;
            config.numAccountsPerGroup = numAccountPerGrain;
            config.numAccounts = numAccountPerGrain * numGrain;
            config.deterministicTxnPercent = 0;
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
            var txns = new List<TxnType>();
            for (int i = 0; i < num_txn_type; i++)
            {
                switch (i)
                {
                    case 0:
                        //txn_type[i] = 0;
                        Console.WriteLine($"Balance {txn_type[i]}");
                        for (int j = 0; j < txn_type[i]; j++) txns.Add(TxnType.Balance);
                        break;
                    case 1:
                        //txn_type[i] = 1;
                        Console.WriteLine($"DepositChecking {txn_type[i]}");
                        for (int j = 0; j < txn_type[i]; j++) txns.Add(TxnType.DepositChecking);
                        break;
                    case 2:
                        //txn_type[i] = 0;
                        Console.WriteLine($"Transfer {txn_type[i]}");
                        for (int j = 0; j < txn_type[i]; j++) txns.Add(TxnType.Transfer);
                        break;
                    case 3:
                        //txn_type[i] = 0;
                        Console.WriteLine($"TransactSaving {txn_type[i]}");
                        for (int j = 0; j < txn_type[i]; j++) txns.Add(TxnType.TransactSaving);
                        break;
                    case 4:
                        //txn_type[i] = 0;
                        Console.WriteLine($"WriteCheck {txn_type[i]}");
                        for (int j = 0; j < txn_type[i]; j++) txns.Add(TxnType.WriteCheck);
                        break;
                    case 5:
                        //txn_type[i] = 0;
                        Console.WriteLine($"MultiTransfer {txn_type[i]}");
                        for (int j = 0; j < txn_type[i]; j++) txns.Add(TxnType.MultiTransfer);
                        break;
                    default:
                        throw new Exception("Unknown transaction type. ");
                }
            }
            var shuffled_txns = txns.OrderBy(x => Guid.NewGuid()).ToList();
            var all_txn_type = new Dictionary<int, TxnType>();
            var commit_txn_type = new int[num_txn_type];
            for (int i = 0; i < num_txn_type; i++) commit_txn_type[i] = 0;
            var tasks = new List<Task<FunctionResult>>();
            for (int i = 0; i < txn_type.Sum(); i++)
            {
                tasks.Add(benchmark.newTransaction(client, global_tid, shuffled_txns[i]));
                all_txn_type.Add(global_tid, shuffled_txns[i]);
                global_tid++;
            }
            try
            {
                await Task.WhenAll(tasks);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}. ");
                return 0;
            }
            var abort = 0;
            foreach (var t in tasks)
            {
                if (t.Result.hasException()) abort++;
                else Debug.Assert(!t.Result.Exp_2PC && !t.Result.Exp_AppLogic && !t.Result.Exp_NotSerializable && !t.Result.Exp_RWConflict && !t.Result.Exp_UnExpect);
            }
            Console.WriteLine($"{abort} txn abroted, abort rate = {100.0 * abort / txn_type.Sum()}%. ");

            // check the final state of all grains
            var ts = new List<Task<FunctionResult>>();
            for (uint i = 0; i < numGrain; i++)
            {
                var groupGUID = Helper.convertUInt32ToGuid(i);
                var grain = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                var accountId = i * numAccountPerGrain;
                var input = new FunctionInput(accountId.ToString());
                input.context = new TransactionContext(global_tid);
                global_tid++;
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
            float real_all_saving = 0;
            float real_all_checking = 0;
            foreach (var t in ts)
            {
                Debug.Assert(!t.Result.hasException());
                real_all_saving += ((CustomerAccountGroup)t.Result.afterState.First().Value).savingAccount.First().Value;
                real_all_checking += ((CustomerAccountGroup)t.Result.afterState.First().Value).checkingAccount.First().Value;
            }

            // check if for every transaction, it's before_state + op = after_state
            foreach (var t in tasks)
            {
                if (!t.Result.hasException())
                {
                    var type = all_txn_type[t.Result.tid];
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
                        else if (type != TxnType.Balance) Debug.Assert(before_saving == after_saving && before_checking == after_checking);
                    }

                    switch (type)
                    {
                        case TxnType.Balance:
                            commit_txn_type[0]++;
                            break;
                        case TxnType.DepositChecking:
                            commit_txn_type[1]++;
                            break;
                        case TxnType.Transfer:
                            commit_txn_type[2]++;
                            break;
                        case TxnType.TransactSaving:
                            commit_txn_type[3]++;
                            break;
                        case TxnType.WriteCheck:
                            commit_txn_type[4]++;
                            break;
                        case TxnType.MultiTransfer:
                            commit_txn_type[5]++;
                            break;
                        default:
                            throw new Exception("Unknown transaction type. ");
                    }
                }
            }

            Console.WriteLine($"Balance commit = {commit_txn_type[0]}, abort = {txn_type[0] - commit_txn_type[0]}");
            Console.WriteLine($"DepositChecking commit = {commit_txn_type[1]}, abort = {txn_type[1] - commit_txn_type[1]}");
            Console.WriteLine($"Transfer commit = {commit_txn_type[2]}, abort = {txn_type[2] - commit_txn_type[2]}");
            Console.WriteLine($"TransactSaving commit = {commit_txn_type[3]}, abort = {txn_type[3] - commit_txn_type[3]}");
            Console.WriteLine($"WriteCheck commit = {commit_txn_type[4]}, abort = {txn_type[4] - commit_txn_type[4]}");
            Console.WriteLine($"MultiTransfer commit = {commit_txn_type[5]}, abort = {txn_type[5] - commit_txn_type[5]}");

            // check the total amount of saving and checking balance
            var expect_all_saving = numGrain * 1000 - commit_txn_type[3];    // TransactSaving
            var expect_all_checking = numGrain * 1000 + commit_txn_type[1] - commit_txn_type[4];  // DepositChecking, WriteCheck
            Debug.Assert(expect_all_saving == real_all_saving);
            Debug.Assert(expect_all_checking >= real_all_checking);

            Console.WriteLine("Finished running experiment. Press Enter to exit");
            Console.ReadLine();

            return 0;
        }

        private static async void InitiateClientAndSpawnConfigurationCoordinator()
        {
            ClientConfiguration config = new ClientConfiguration();
            client = await config.StartClientWithRetries();

            exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), 1000);
            coordConfig = new CoordinatorGrainConfiguration(100, 10000, 120, 1);
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
                input.context = new TransactionContext(global_tid);
                global_tid++;
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
