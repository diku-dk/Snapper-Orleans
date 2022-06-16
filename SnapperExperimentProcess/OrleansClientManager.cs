using System;
using Orleans;
using Utilities;
using Orleans.Runtime;
using Orleans.Hosting;
using Orleans.Configuration;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics;
using SmallBank.Interfaces;
using TPCC.Interfaces;
using Orleans.Transactions;
using System.IO;

namespace SnapperExperimentProcess
{
    public class OrleansClientManager
    {
        readonly string ServiceRegion;
        readonly string AccessKey;
        readonly string SecretKey;
        readonly SiloConfiguration siloConfig;
        static IClusterClient client;

        public OrleansClientManager(SiloConfiguration siloConfig)
        {
            this.siloConfig = siloConfig;
            using (var file = new StreamReader(Constants.credentialFile))
            {
                ServiceRegion = file.ReadLine();
                AccessKey = file.ReadLine();
                SecretKey = file.ReadLine();
            }
        }

        public async Task<IClusterClient> StartClientWithRetries(int initializeAttemptsBeforeFailing = 5)
        {
            int attempt = 0;
            while (true)
            {
                try
                {
                    var clientBuilder = new ClientBuilder()
                        .Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = Constants.ClusterSilo;
                            options.ServiceId = Constants.ServiceID;
                        });

                    if (Constants.LocalCluster) clientBuilder.UseLocalhostClustering();
                    else
                    {
                        Action<DynamoDBGatewayOptions> dynamoDBOptions = options => {
                            options.AccessKey = AccessKey;
                            options.SecretKey = SecretKey;
                            options.TableName = Constants.SiloMembershipTable;
                            options.Service = ServiceRegion;
                            options.WriteCapacityUnits = 10;
                            options.ReadCapacityUnits = 10;

                        };

                        clientBuilder.UseDynamoDBClustering(dynamoDBOptions);
                    }
                    
                    client = clientBuilder.Build();
                    await client.Connect();
                    Console.WriteLine("Client successfully connect to silo host");
                    break;
                }
                catch (SiloUnavailableException)
                {
                    attempt++;
                    Console.WriteLine($"Attempt {attempt} of {initializeAttemptsBeforeFailing} failed to initialize the Orleans client.");
                    if (attempt > initializeAttemptsBeforeFailing)
                    {
                        throw;
                    }
                    await Task.Delay(TimeSpan.FromSeconds(4));
                }
            }
            return client;
        }

        public async Task LoadSmallBankGrains(bool loggingEnabled)
        {
            Debug.Assert(client != null);
            var numGrain = Helper.GetNumGrainPerSilo(siloConfig.numCPUPerSilo);
            Console.WriteLine($"Load {numGrain} Smallbank grains...");
            var tasks = new List<Task>();
            var sequence = false;   // load the grains in sequence instead of all concurrent
            if (loggingEnabled) sequence = true;
            var start = DateTime.Now;
            for (int i = 0; i < numGrain; i++)
            {
                var input = new Tuple<int, int>(1, i);
                switch (siloConfig.implementationType)
                {
                    case ImplementationType.NONTXN:
                        var etxnGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(i);
                        tasks.Add(etxnGrain.StartTransaction("Init", input));
                        break;
                    case ImplementationType.ORLEANSTXN:
                        var orltxnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(i);
                        tasks.Add(orltxnGrain.StartTransaction("Init", input));
                        break;
                    case ImplementationType.SNAPPER:
                        var sntxnGrain = client.GetGrain<ICustomerAccountGroupGrain>(i);
                        tasks.Add(sntxnGrain.StartTransaction("Init", input));
                        break;
                    default:
                        throw new Exception("Unknown grain implementation type");
                }

                if (sequence && tasks.Count == 200)
                {
                    //Console.WriteLine($"Load {Environment.ProcessorCount} grains, i = {i}");
                    await Task.WhenAll(tasks);
                    tasks.Clear();
                }
            }
            if (tasks.Count > 0) await Task.WhenAll(tasks);
            Console.WriteLine($"Finish loading grains, it takes {(DateTime.Now - start).TotalSeconds}s.");
        }

        public async Task LoadTPCCGrains()
        {
            Debug.Assert(client != null);
            Debug.Assert(siloConfig.implementationType != ImplementationType.ORLEANSTXN);
            var eventual = siloConfig.implementationType == ImplementationType.NONTXN;
            Console.WriteLine($"Load TPCC grains...");
            var sequence = true;   // load the grains in sequence instead of all concurrent
            var start = DateTime.Now;

            var tpccManager = new TPCCManager();
            tpccManager.Init(siloConfig.numCPUPerSilo, siloConfig.NUM_OrderGrain_PER_D);

            // load ItemGrain
            var NUM_W_PER_SILO = Helper.GetNumWarehousePerSilo(siloConfig.numCPUPerSilo);
            for (int W_ID = 0; W_ID < NUM_W_PER_SILO; W_ID++)
            {
                var grainID = tpccManager.GetItemGrain(W_ID);
                if (eventual)
                {
                    var grain = client.GetGrain<IEventualItemGrain>(grainID);
                    await grain.StartTransaction("Init", null);
                }
                else
                {
                    var grain = client.GetGrain<IItemGrain>(grainID);
                    await grain.StartTransaction("Init", null);
                }
            }
            Console.WriteLine($"Finish loading {NUM_W_PER_SILO} ItemGrain. ");

            // load WarehouseGrain
            for (int W_ID = 0; W_ID < NUM_W_PER_SILO; W_ID++)
            {
                var grainID = tpccManager.GetWarehouseGrain(W_ID);
                if (eventual)
                {
                    var grain = client.GetGrain<IEventualWarehouseGrain>(grainID);
                    await grain.StartTransaction("Init", W_ID);
                }
                else
                {
                    var grain = client.GetGrain<IWarehouseGrain>(grainID);
                    await grain.StartTransaction("Init", W_ID);
                }
            }
            Console.WriteLine($"Finish loading {NUM_W_PER_SILO} WarehouseGrain. ");

            // load DistrictGrain and CustomerGrain
            int districtGrainID;
            int customerGrainID;
            var tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    districtGrainID = tpccManager.GetDistrictGrain(W_ID, D_ID);
                    customerGrainID = tpccManager.GetCustomerGrain(W_ID, D_ID);
                    var input = new Tuple<int, int>(W_ID, D_ID);
                    if (eventual)
                    {
                        var districtGrain = client.GetGrain<IEventualDistrictGrain>(districtGrainID);
                        tasks.Add(districtGrain.StartTransaction("Init", input));
                        var customerGrain = client.GetGrain<IEventualCustomerGrain>(customerGrainID);
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
            Console.WriteLine($"Finish loading {NUM_W_PER_SILO * Constants.NUM_D_PER_W} DistrictGrain and {NUM_W_PER_SILO * Constants.NUM_D_PER_W} CustomerGrain. ");

            // load StockGrain
            int stockGrainID;
            tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < NUM_W_PER_SILO; W_ID++)
            {
                for (int i = 0; i < Constants.NUM_StockGrain_PER_W; i++)
                {
                    stockGrainID = W_ID * tpccManager.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + i;
                    var input = new Tuple<int, int>(W_ID, i);
                    if (eventual)
                    {
                        var grain = client.GetGrain<IEventualStockGrain>(stockGrainID);
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
            Console.WriteLine($"Finish loading {NUM_W_PER_SILO * Constants.NUM_StockGrain_PER_W} StockGrain. ");

            // load OrderGrain
            int orderGrainID;
            tasks = new List<Task<TransactionResult>>();
            for (int W_ID = 0; W_ID < NUM_W_PER_SILO; W_ID++)
            {
                for (int D_ID = 0; D_ID < Constants.NUM_D_PER_W; D_ID++)
                {
                    for (int i = 0; i < tpccManager.NUM_OrderGrain_PER_D; i++)
                    {
                        orderGrainID = W_ID * tpccManager.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * tpccManager.NUM_OrderGrain_PER_D + i;
                        var input = new Tuple<int, int, int>(W_ID, D_ID, i);
                        if (eventual)
                        {
                            var grain = client.GetGrain<IEventualOrderGrain>(orderGrainID);
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
            Console.WriteLine($"Finish loading {NUM_W_PER_SILO * Constants.NUM_D_PER_W * tpccManager.NUM_OrderGrain_PER_D} OrderGrain. ");

            Console.WriteLine($"Finish loading TPCC grains, it takes {(DateTime.Now - start).TotalSeconds}s.");
        }
    }
}