using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TPCC.Interfaces;
using SnapperExperimentProcess;
using SmallBank.Interfaces;
using Orleans;
using Concurrency.Interface.Configuration;
using Utilities;
using System.Diagnostics;
using System.Threading;

namespace SnapperExperimentController
{
    public class ServerConnector
    {
        IClusterClient client;
        IGlobalConfigGrain globalConfigGrain;

        bool loadingDone;
        bool initializationFinish;
        bool resetFinish;
        bool checkGCFinish;

        public ServerConnector()
        {
            loadingDone = false;
            initializationFinish = false;
            resetFinish = false;
            checkGCFinish = false;
        }

        public void InitiateClientAndServer()
        {
            InitiateClientAndServerAsync();
            while (!initializationFinish) Thread.Sleep(100);
        }

        public void LoadGrains()
        {
            if (Constants.benchmark == BenchmarkType.SMALLBANK) LoadSmallBankGrains();
            else if (Constants.benchmark == BenchmarkType.TPCC) LoadTPCCGrains();
            while (!loadingDone) Thread.Sleep(100);
        }

        async void InitiateClientAndServerAsync()
        {
            var manager = new OrleansClientManager();
            client = await manager.StartOrleansClient();
            
            if (Constants.implementationType == ImplementationType.SNAPPER)
            {
                globalConfigGrain = client.GetGrain<IGlobalConfigGrain>(0);
                await globalConfigGrain.ConfigGlobalEnv();
                Console.WriteLine($"Spawned the global configuration grain.");
            }
            initializationFinish = true;
        }

        public void CheckGC()
        {
            CheckGCAsync();
            while (!checkGCFinish) Thread.Sleep(100);
            checkGCFinish = false;
        }

        async void CheckGCAsync()
        {
            if (Constants.implementationType != ImplementationType.SNAPPER) return;
            if (Constants.benchmark != BenchmarkType.SMALLBANK) return;

            // check all global & local coordinators
            var tasks = new List<Task>();
            await globalConfigGrain.CheckGC();

            // check all transactional grains
            for (int i = 0; i < Constants.numGrainPerSilo * Constants.numSilo; i++)
            {
                var grain = client.GetGrain<ISnapperTransactionalAccountGrain>(i);
                tasks.Add(grain.CheckGC());
            }

            await Task.WhenAll(tasks);
            checkGCFinish = true;
        }

        public void ResetOrderGrain()
        {
            if (Constants.benchmark != BenchmarkType.TPCC) return;
            ResetOrderGrainAsync();
            while (!resetFinish) Thread.Sleep(100);
            resetFinish = false;
        }

        async void ResetOrderGrainAsync()
        {
            // set OrderGrain as empty table
            var index = 0;
            var tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    for (int i = 0; i < Constants.NUM_OrderGrain_PER_D; i++)
                    {
                        index = W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * Constants.NUM_OrderGrain_PER_D + i;
                        var input = new Tuple<int, int, int>(W_ID, D_ID, i);
                        if (Constants.implementationType == ImplementationType.ORLEANSEVENTUAL)
                        {
                            var grain = client.GetGrain<INTOrderGrain>(index);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        else
                        {
                            var grain = client.GetGrain<IOrderGrain>(index);
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
            Console.WriteLine($"Finish re-setting OrderGrain. ");
            resetFinish = true;
        }

        async void LoadSmallBankGrains()
        {
            int numGrain = Constants.numGrainPerSilo * Constants.numSilo;
            Console.WriteLine($"Load SmallBank grains, numGrains = {numGrain}");
            var tasks = new List<Task<TransactionResult>>();
            var sequence = false;   // load the grains in sequence instead of all concurrent
            if (Constants.loggingType != LoggingType.NOLOGGING) sequence = true;
            var start = DateTime.Now;
            for (int i = 0; i < numGrain; i++)
            {
                switch (Constants.implementationType)
                {
                    case ImplementationType.ORLEANSEVENTUAL:
                        var etxnGrain = client.GetGrain<INonTransactionalAccountGrain>(i);
                        tasks.Add(etxnGrain.StartTransaction("Init", i));
                        break;
                    case ImplementationType.ORLEANSTXN:
                        var orltxnGrain = client.GetGrain<IOrleansTransactionalAccountGrain>(i);
                        tasks.Add(orltxnGrain.StartTransaction("Init", i));
                        break;
                    case ImplementationType.SNAPPER:
                        var sntxnGrain = client.GetGrain<ISnapperTransactionalAccountGrain>(i);
                        tasks.Add(sntxnGrain.StartTransaction("Init", i));
                        break;
                    default:
                        throw new Exception("Unknown grain implementation type");
                }

                if (sequence && tasks.Count == Constants.numGrainPerSilo / 10)
                {
                    Console.WriteLine($"Load {Environment.ProcessorCount} grains, i = {i}");
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading grains, it takes {(DateTime.Now - start).TotalSeconds}s.");
            loadingDone = true;
        }

        async void LoadTPCCGrains()
        {
            Debug.Assert(Constants.implementationType != ImplementationType.ORLEANSTXN);
            var eventual = Constants.implementationType == ImplementationType.ORLEANSEVENTUAL;
            Console.WriteLine($"Load TPCC grains, {Constants.implementationType}. ");
            var sequence = true;   // load the grains in sequence instead of all concurrent
            var start = DateTime.Now;

            // load ItemGrain
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
            {
                var grainID = Helper.GetItemGrain(W_ID);
                if (eventual)
                {
                    var grain = client.GetGrain<INTItemGrain>(grainID);
                    await grain.StartTransaction("Init", null);
                }
                else
                {
                    var grain = client.GetGrain<IItemGrain>(grainID);
                    await grain.StartTransaction("Init", null);
                }
            }
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO} ItemGrain. ");

            // load WarehouseGrain
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
            {
                var grainID = Helper.GetWarehouseGrain(W_ID);
                if (eventual)
                {
                    var grain = client.GetGrain<INTWarehouseGrain>(grainID);
                    await grain.StartTransaction("Init", W_ID);
                }
                else
                {
                    var grain = client.GetGrain<IWarehouseGrain>(grainID);
                    await grain.StartTransaction("Init", W_ID);
                }
            }
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO} WarehouseGrain. ");

            // load DistrictGrain and CustomerGrain
            int districtGrainID;
            int customerGrainID;
            var tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    districtGrainID = Helper.GetDistrictGrain(W_ID, D_ID);
                    customerGrainID = Helper.GetCustomerGrain(W_ID, D_ID);
                    var input = new Tuple<int, int>(W_ID, D_ID);
                    if (eventual)
                    {
                        var districtGrain = client.GetGrain<INTDistrictGrain>(districtGrainID);
                        tasks.Add(districtGrain.StartTransaction("Init", input));
                        var customerGrain = client.GetGrain<INTCustomerGrain>(customerGrainID);
                        tasks.Add(customerGrain.StartTransaction("Init", input));
                    }
                    else
                    {
                        var districtGrain = client.GetGrain<IDistrictGrain>(districtGrainID);
                        tasks.Add(districtGrain.StartTransaction("Init", input));
                        var customerGrain = client.GetGrain<ICustomerGrain>(customerGrainID);
                        tasks.Add(customerGrain.StartTransaction("Init", input));
                    }
                    if (sequence && tasks.Count == Environment.ProcessorCount)
                    {
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO * Constants.NUM_D_PER_W} DistrictGrain and {Constants.NUM_W_PER_SILO * Constants.NUM_D_PER_W} CustomerGrain. ");

            // load StockGrain
            int stockGrainID;
            tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
            {
                for (int i = 0; i < Constants.NUM_StockGrain_PER_W; i++)
                {
                    stockGrainID = W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + i;
                    var input = new Tuple<int, int>(W_ID, i);
                    if (eventual)
                    {
                        var grain = client.GetGrain<INTStockGrain>(stockGrainID);
                        tasks.Add(grain.StartTransaction("Init", input));
                    }
                    else
                    {
                        var grain = client.GetGrain<IStockGrain>(stockGrainID);
                        tasks.Add(grain.StartTransaction("Init", input));
                    }
                    if (sequence && tasks.Count == Environment.ProcessorCount)
                    {
                        await Task.WhenAll(tasks);
                        tasks.Clear();
                    }
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO * Constants.NUM_StockGrain_PER_W} StockGrain. ");

            // load OrderGrain
            int orderGrainID;
            tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < Constants.NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    for (int i = 0; i < Constants.NUM_OrderGrain_PER_D; i++)
                    {
                        orderGrainID = W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * Constants.NUM_OrderGrain_PER_D + i;
                        var input = new Tuple<int, int, int>(W_ID, D_ID, i);
                        if (eventual)
                        {
                            var grain = client.GetGrain<INTOrderGrain>(orderGrainID);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        else
                        {
                            var grain = client.GetGrain<IOrderGrain>(orderGrainID);
                            tasks.Add(grain.StartTransaction("Init", input));
                        }
                        if (sequence && tasks.Count == Environment.ProcessorCount)
                        {
                            await Task.WhenAll(tasks);
                            tasks.Clear();
                        }
                    }
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading {Constants.NUM_W_PER_SILO * Constants.NUM_D_PER_W * Constants.NUM_OrderGrain_PER_D} OrderGrain. ");

            Console.WriteLine($"Finish loading grains, it takes {(DateTime.Now - start).TotalSeconds}s.");
            loadingDone = true;
        }
    }
}
