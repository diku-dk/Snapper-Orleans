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
                            options.ServiceId = "AccountTransferApp";
                        })
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
                            options.ClusterId = "dev";
                            options.ServiceId = "AccountTransferApp";
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

            var Test = new GlobalCoordinatorTest(2, client);
            await Test.SpawnCoordinator();
            //await RunPerformanceTestOnThroughput(client);
            //await TestTransaction(client);
        }

        
        private static async Task RunPerformanceTestOnThroughput(IClusterClient client)
        {
            TestThroughput test = new TestThroughput(n1, n2);
            if (initActor)
                await test.initializeGrain(client);
            await test.DoTest(client, N, false);

        }

        private static async Task TestTransaction(IClusterClient client)
        {
            bool sequential = true;
            int numTransfer = 100;
            IAccountGrain fromAccount = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(1));
            IAccountGrain toAccount = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(2));
            IATMGrain atm = client.GetGrain<IATMGrain>(Helper.convertUInt32ToGuid(3));

            Guid fromId = fromAccount.GetPrimaryKey();
            Guid toId = toAccount.GetPrimaryKey();
            Guid atmId = atm.GetPrimaryKey();

            var grainAccessInformation = new Dictionary<Guid, Tuple<String, int>>();
            grainAccessInformation.Add(fromId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
            grainAccessInformation.Add(toId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
            grainAccessInformation.Add(atmId, new Tuple<string, int>("AccountTransfer.Grains.ATMGrain", 1));

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
                    var task = atm.StartTransaction("Transfer", input);
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

            //Deterministic Transactions

            //try
            //{
            //    Task t1 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
            //    Task t2 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
            //    Task t3 = atm.StartTransaction(grainAccessInformation, "Transfer", input);

            //    await Task.WhenAll(t1, t2, t3);
            //}
            //catch (Exception e)
            //{
            //    Console.WriteLine($"\n\n {e.ToString()}\n\n");
            //}


        }


    }
}
