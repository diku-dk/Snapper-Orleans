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
            await TestTransaction(client);

        }

        private static async void TestThroughputLatency(IClusterClient client)
        {
            //initialize 100 ATM and 10000 accounts
            Dictionary<int, IATMGrain> atmMap = new Dictionary<int, IATMGrain>();
            Dictionary<int, IAccountGrain> accountMap = new Dictionary<int, IAccountGrain>();

            //DateTime ts1 = DateTime.Now;

            //ATM id ranges from 1 to 100;
            //for (int i = 1; i <= 100; i++)
            //{
            //    IATMGrain atm = client.GetGrain<IATMGrain>(i);
            //    await atm.ActivateGrain();
            //    atmMap.Add(i, atm);
            //}

            //Account id ranges from 101 to 10100
            //for (int i = 101; i <= 10100; i++)
            //{
            //    IAccountGrain account = client.GetGrain<IAccountGrain>(i);
            //    await account.ActivateGrain();
            //    accountMap.Add(i, account);
            //}

            //DateTime ts2 = DateTime.Now;
            //Console.WriteLine($"\n\n Initialization time: {ts2 - ts1}.\n\n");

            List<List<int>> grainsPerTx = new List<List<int>>();
            Random rand = new Random();
            int N = 10000;

            for(int i=0; i<N; i++)
            {
                grainsPerTx.Add(getGrains(rand));
            }

            DateTime ts3 = DateTime.Now;
            List <Task> Txs= new List<Task>();


            for (int i = 0; i < N; i++)
            {
                List<int> grains = grainsPerTx[i];
                IATMGrain atm = client.GetGrain<IATMGrain>(grains[0]);
                IAccountGrain from = client.GetGrain<IAccountGrain>(grains[1]);
                IAccountGrain to = client.GetGrain<IAccountGrain>(grains[2]);
                //IATMGrain atm = atmMap[grains[0]]; 
                //IAccountGrain from = accountMap[grains[1]];
                //IAccountGrain to = accountMap[grains[2]];

                Dictionary<ITransactionExecutionGrain, int> grainToAccessTimes = new Dictionary<ITransactionExecutionGrain, int>();
                grainToAccessTimes.Add(from, 1);
                grainToAccessTimes.Add(to, 1);
                grainToAccessTimes.Add(atm, 1);

                Txs.Add(atm.StartTransaction(grainToAccessTimes, "Transfer", new List<object>() { from, to, 100 }));

                if (i % 1000 == 0)
                {
                    System.Threading.Thread.Sleep(1000);
                }
            }

            await Task.WhenAll(Txs);
            DateTime ts4 = DateTime.Now;

            Console.WriteLine($"\n\n Processed {N} transactions using: {ts4 - ts3}.\n\n");

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

        private static async void TestOrleansThroughputLatency(IClusterClient client)
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
#pragma warning disable CS0168 // Variable is declared but never used
            catch(Exception e){
#pragma warning restore CS0168 // Variable is declared but never used
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

        }


        private static async Task TestTransaction(IClusterClient client)
        {
            Guid from = new Guid("ad3e2c63-2e4c-4bcb-b065-81b3768e2c72");
            Guid to = new Guid("28d414fa-4164-4a76-99b1-ab1d0c5edf2f");

            //IATMGrain atm = client.GetGrain<IATMGrain>(atmId);
            IAccountGrain fromAccount = client.GetGrain<IAccountGrain>(1);
            IAccountGrain toAccount = client.GetGrain<IAccountGrain>(2);



            Guid atmId0 = new Guid("ad3e2c63-2e4c-4bcb-b065-81b3768e2c98");
            IATMGrain atm0 = client.GetGrain<IATMGrain>(3);

            Dictionary<Guid, int> grainToAccessTimes = new Dictionary<Guid, int>();
            grainToAccessTimes.Add(from, 1);
            grainToAccessTimes.Add(to, 1);
            grainToAccessTimes.Add(atmId0, 1);
            List<object> inputArgs = new List<Object> {from, to, atmId0 , 100};
            FunctionInput input = new FunctionInput(inputArgs);
            try
            {
                Task t1 = atm0.StartTransaction(grainToAccessTimes, "Transfer", input);
                Task t2 = atm0.StartTransaction(grainToAccessTimes, "Transfer", input);
                Task t3 = atm0.StartTransaction(grainToAccessTimes, "Transfer", input);

                await Task.WhenAll(t1, t2, t3);
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n\n {e.ToString()}\n\n");
            }
            Console.WriteLine($"\n\n {fromAccount.GetBalance().Result}  , {toAccount.GetBalance().Result}\n\n");
        }
    }
}
