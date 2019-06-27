using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using AccountTransfer.Interfaces;
using System.Net;
using Orleans.Configuration;
using System.Collections.Generic;
using AccountTransfer.Grains;
using System.Reflection;
using Concurrency.Interface;
using Utilities;
using System.Threading;
using Utilities;
using Orleans.Hosting;
using Concurrency.Interface.Nondeterministic;
using SmallBank.Interfaces;

namespace OrleansClient
{
    /// <summary>
    /// Orleans test silo client
    /// </summary>
    public class Program
    {
        static uint n1, n2;
        static int N;
        static Boolean initActor;
        static int Main(string[] args)
        {
 

            n1 = Convert.ToUInt32(args[0]);
            n2 = Convert.ToUInt32(args[1]);
            N = Convert.ToInt32(args[2]);
            initActor = Convert.ToBoolean(args[3]);
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            try
            {
                using (var client = await StartClientWithRetries())
                //using (var client = await StartClientWithRetriesToCluster())
                {
                    await DoClientWork(client);
                    Console.ReadKey();
                }

                return 0;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                return 1;
            }
        }

        private static async Task<IClusterClient> StartClientWithRetries(int initializeAttemptsBeforeFailing = 5)
        {
            int attempt = 0;
            IClusterClient client;
            while (true)
            {
                try
                {
                    Thread.Sleep(5000);

                    client = new ClientBuilder()
                        .UseLocalhostClustering()
                        .Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = "dev";
                            options.ServiceId = "Snapper";
                        })
                        .ConfigureLogging(logging => logging.AddConsole())
                        .Build();

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

        private static async Task<IClusterClient> StartClientWithRetriesToCluster(int initializeAttemptsBeforeFailing = 5)
        {
            int attempt = 0;
            IClusterClient client;
            while (true)
            {
                try
                {   
                    

                    Action<DynamoDBGatewayOptions> dynamoDBOptions = options => {
                        options.AccessKey = "AKIAJILO2SVPTNUZB55Q";
                        options.SecretKey = "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";
                        options.TableName = "XLibMembershipTable";
                        options.Service = "eu-west-1";
                        options.WriteCapacityUnits = 10;
                        options.ReadCapacityUnits = 10;
                        
                    };   

                    client = new ClientBuilder()
                        .Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = "ec2";
                            options.ServiceId = "Snapper";
                        })
                        .UseDynamoDBClustering(dynamoDBOptions)
                        .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccountGrain).Assembly).WithReferences())
                        .ConfigureLogging(logging => logging.AddConsole())
                        .Build();
                    
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


        private static async Task DoClientWork(IClusterClient client)
        {

            //var Test = new GlobalCoordinatorTest(client);
            //await Test.SpawnCoordinator();
            //await Test.ConcurrentDetTransaction();
            await MiniBankMultiTransfer(client);
        }

        private static async Task MiniBankMultiTransfer(IClusterClient client)
        {

             IConfigurationManagerGrain configGrain;
             Random rand = new Random();
              uint numOfCoordinators = 5;
              int batchIntervalMsecs = 1000;
              int backoffIntervalMsecs = 1000;
              int idleIntervalTillBackOffSecs = 10;
             uint numGroup = 10;
            uint numAccountsPerGroup = 100;
             int maxNonDetWaitingLatencyInMs = 1000;
             ConcurrencyType nonDetCCType = ConcurrencyType.TIMESTAMP;

            //Spawn Configuration grain
            configGrain = client.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
            var exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), maxNonDetWaitingLatencyInMs);
            var coordConfig = new CoordinatorGrainConfiguration(batchIntervalMsecs, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numOfCoordinators);
            await configGrain.UpdateNewConfiguration(exeConfig);
            await configGrain.UpdateNewConfiguration(coordConfig);

            List<Task<FunctionResult>> tasks = new List<Task<FunctionResult>>();
            for (uint i = 0; i < numGroup; i++)
            {
                var localArgs = new Tuple<uint, uint>(numAccountsPerGroup, i);
                var localInput = new FunctionInput(localArgs);
                ICustomerAccountGroupGrain target = client.GetGrain<ICustomerAccountGroupGrain>(Helper.convertUInt32ToGuid(i));
                tasks.Add(target.StartTransaction("InitBankAccounts", localInput));
            }
            await Task.WhenAll(tasks);


            uint numDestinationAccount = 2;
            uint sourceID = 201;
            Tuple<String, UInt32> item1 = new Tuple<string, uint>(sourceID.ToString(), sourceID);
            float item2 = 10;
            List<Tuple<string, uint>> item3 = new List<Tuple<string, uint>>();
            List<uint> destinationIDs = new List<uint>();
            for (uint i = 0; i < numDestinationAccount; i++)
            {
                uint destAccountID = 301;
                item3.Add(new Tuple<string, uint>(destAccountID.ToString(), destAccountID));
            }
            var args = new Tuple<Tuple<String, UInt32>, float, List<Tuple<String, UInt32>>>(item1, item2, item3);
            var input = new FunctionInput(args);
            var groupGUID = Helper.convertUInt32ToGuid(sourceID/ numAccountsPerGroup);
            var destination = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
            Task<FunctionResult> task = destination.StartTransaction("MultiTransfer", input);
            await task;


        }


        private static async Task TestTransaction(IClusterClient client)
        {
            bool sequential = true;
            int numTransfer = 100;
            IAccountGrain fromAccount = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(1));
            IAccountGrain toAccount = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(2));            

            Guid fromId = fromAccount.GetPrimaryKey();
            Guid toId = toAccount.GetPrimaryKey();

            var grainAccessInformation = new Dictionary<Guid, Tuple<String, int>>();
            grainAccessInformation.Add(fromId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
            grainAccessInformation.Add(toId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));            

            var args = new TransferInput(1, 2, 10);
            FunctionInput input = new FunctionInput(args);
            //Non - deterministic transaction
            int count = 0;

            try
            {
                Task<FunctionResult> t1 = fromAccount.StartTransaction("GetBalance", input);
                await t1;                
                Task<FunctionResult> t2 = toAccount.StartTransaction("GetBalance", input);
                await t2;                
                if (!t1.Result.hasException() && !t2.Result.hasException())
                    Console.WriteLine($"Pre transfer balances(src, dest) = ({t1.Result.resultObject},{t2.Result.resultObject})\n");
                else
                    Console.WriteLine("These getBalance txns should not abort");

                Console.WriteLine($"Performing {numTransfer} transfers each of value {args.transferAmount} with sequential mode {sequential}");
                List<Task<FunctionResult>> tasks = new List<Task<FunctionResult>>();
                for (int i = 0; i < numTransfer; i++)
                {
                    var task = fromAccount.StartTransaction("Transfer", input);
                    tasks.Add(task);
                    if (sequential)
                    {
                        await task;
                    }
                        
                }

                if (!sequential)
                {
                    await Task.WhenAll(tasks);                    
                }
                foreach (var aResultTask in tasks)
                {
                    if (!aResultTask.Result.hasException())
                    {
                        count++;
                    }
                }

                Console.WriteLine($"Finished {count} transfer txns successfully");
                Task<FunctionResult> t5 = fromAccount.StartTransaction("GetBalance", input);
                await t5;
                Task<FunctionResult> t6 = toAccount.StartTransaction("GetBalance", input);
                await t6;
                if (!t5.Result.hasException() && !t6.Result.hasException())
                    Console.WriteLine($"Post transfer balances ({t5.Result.resultObject},{t6.Result.resultObject})");
                else
                    Console.WriteLine("These getBalance txns should not abort");
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n\n {e.ToString()}\n\n");
            }
        }


    }
}
