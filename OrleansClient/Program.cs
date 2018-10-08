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
using Concurrency.Utilities;
using System.Threading;
using Utilities;

namespace OrleansClient
{
    /// <summary>
    /// Orleans test silo client
    /// </summary>
    public class Program
    {
        static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            try
            {
                using (var client = await StartClientWithRetries())
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
                    int gatewayPort = 30000;
                    var siloAddress = IPAddress.Loopback;
                    var gateway = new IPEndPoint(siloAddress, gatewayPort);

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

        private static async Task DoClientWork(IClusterClient client)
        {
            TestThroughput test = new TestThroughput(10, 100);
            await test.DoTest(client, 100, false);
            //await TestTransaction(client);
            return;
        }

        private static async Task TestTransaction(IClusterClient client)
        {
            bool sequential = false;
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

        private static List<int> getGrains(Random rand)
        {
            List<int> ret = new List<int>();
            int atm = rand.Next(1, 10000);
            int from = rand.Next(10000, 1000000);
            int to = rand.Next(10000, 1000000);
            while (from == to)
            {
                to = rand.Next(101, 10100);
            }
            ret.Add(atm);
            ret.Add(from);
            ret.Add(to);

            return ret;
        }

        /*private static async void TestOrleansThroughputLatency(IClusterClient client)
        {

            List<List<int>> grainsPerTx = new List<List<int>>();
            Random rand = new Random();
            int N = 10;

            for (int i = 0; i < N; i++)
            {
                grainsPerTx.Add(getGrains(rand));
            }

            DateTime ts3 = DateTime.Now;
            List<Task> Txs = new List<Task>();

            int n = 0;
            try
            {
                for (int i = 0; i < N; i++)
                {
                    List<int> grains = grainsPerTx[i];
                    IOrleansATM atm = client.GetGrain<IOrleansATM>(grains[0]);
                    Txs.Add(atm.Transfer(grains[1], grains[2], 100));
                    if (i % 1000 == 0)
                    {
                        System.Threading.Thread.Sleep(1000);
                    }
                }

                await Task.WhenAll(Txs);
            }
            catch(Exception ){
                n++;
            }

            DateTime ts4 = DateTime.Now;

            int m = 0;
            foreach(Task t in Txs)
            {
                if (t.IsCompletedSuccessfully)
                    m++;
            }
            Console.WriteLine($"\n\n Processed {N} transactions using: {ts4 - ts3}. errors {n}, completed transactions: {m}.\n\n");

        }*/



    }
}
