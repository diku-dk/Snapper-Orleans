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

namespace OrleansClient
{
    /// <summary>
    /// Throughput tests
    /// </summary>
    public class TestThroughput
    {
        // n1 = number of ATMs
        // n2 = number of accounts;
        //  N = number of transactions
        uint numAccounts;
        int N;


        public TestThroughput(uint numAccounts)
        {            
            this.numAccounts = numAccounts;

        }

        public async Task DoTest(IClusterClient client, int nTx, bool isDeterministic)
        {

            //initialize n1 ATM and n2 accounts
            //await initializeGrain();
            this.N = nTx;

            var grainsPerTx = new List<Tuple<uint,uint>>();
            //getGrainsNoContention(grainsPerTx);

            Random rand = new Random();
            for (uint i = 0; i < N; i++)
            {
                grainsPerTx.Add(getGrains(numAccounts, rand));
                //Console.WriteLine($"Grains in transaction: {grainsPerTx[grainsPerTx.Count-1][0]}, {grainsPerTx[grainsPerTx.Count - 1][1]}, {grainsPerTx[grainsPerTx.Count - 1][2]}\n");
            }

            Console.WriteLine($"\n\n Start running Transactions ....\n\n");
            var checkBalance = true;
            try
            {
                List<Task<FunctionResult>> tasks = new List<Task<FunctionResult>>();
                DateTime ts1 = DateTime.Now;

                for (int i = 0; i < N; i++)
                {
                    var grains = grainsPerTx[i];
                    var balanceTasks = new List<Task>();
                    if (checkBalance)
                    {

                        var fromId = Helper.convertUInt32ToGuid(grains.Item1);
                        var toId = Helper.convertUInt32ToGuid(grains.Item2);
                        var fromAccount = client.GetGrain<IAccountGrain>(fromId);
                        var toAccount = client.GetGrain<IAccountGrain>(toId);
                        Task<FunctionResult> t1 = fromAccount.StartTransaction("GetBalance", new FunctionInput());
                        balanceTasks.Add(t1);
                        Task<FunctionResult> t2 = toAccount.StartTransaction("GetBalance", new FunctionInput());
                        balanceTasks.Add(t2);
                        await Task.WhenAll(balanceTasks);
                    }
                }

                for (int i = 0; i < N; i++)
                {
                    var grains = grainsPerTx[i];

                    if (isDeterministic)
                    {
                        var grainAccessInformation = new Dictionary<Guid, Tuple<String, int>>();                        
                        Guid from = Helper.convertUInt32ToGuid(grains.Item1);
                        Guid to = Helper.convertUInt32ToGuid(grains.Item2);

           
                        grainAccessInformation.Add(from, new Tuple<String, int>("AccountTransfer.Grains.AccountGrain", 1));
                        grainAccessInformation.Add(to, new Tuple<String, int>("AccountTransfer.Grains.AccountGrain", 1));

                        IAccountGrain sourceGrain = client.GetGrain<IAccountGrain>(from);
                        var args = new TransferInput(grains.Item1, grains.Item2, 10);
                        FunctionInput input = new FunctionInput(args);
                        //System.Threading.Thread.Sleep(10);
                        tasks.Add(sourceGrain.StartTransaction(grainAccessInformation, "Transfer", input));
                        //await atm.StartTransaction(grainAccessInformation, "Transfer", input);
                    }
                    else
                    {
                        IAccountGrain sourceGrain = client.GetGrain<IAccountGrain>((Helper.convertUInt32ToGuid(grains.Item1)));
                        var args = new TransferInput(grains.Item1, grains.Item2, 10);
                        //var args = new TransferInput(13, 14, 10);
                        FunctionInput input = new FunctionInput(args);
                        Task<FunctionResult> task = sourceGrain.StartTransaction("Transfer", input);
                        tasks.Add(task);
                    }
                }
                await Task.WhenAll(tasks);
                DateTime ts2 = DateTime.Now;

                for (int i = 0; i < N; i++)
                {
                    var grains = grainsPerTx[i];
                    var balanceTasks = new List<Task>();
                    if (checkBalance)
                    {

                        var fromId = Helper.convertUInt32ToGuid(grains.Item1);
                        var toId = Helper.convertUInt32ToGuid(grains.Item2);
                        var fromAccount = client.GetGrain<IAccountGrain>(fromId);
                        var toAccount = client.GetGrain<IAccountGrain>(toId);
                        Task<FunctionResult> t1 = fromAccount.StartTransaction("GetBalance", new FunctionInput());
                        balanceTasks.Add(t1);
                        Task<FunctionResult> t2 = toAccount.StartTransaction("GetBalance", new FunctionInput());
                        balanceTasks.Add(t2);
                        await Task.WhenAll(balanceTasks);
                    }
                }

                int count = 0;
                foreach (var aResultTask in tasks)
                {
                    if (!aResultTask.Result.hasException())
                    {
                        count++;
                    }
                }
                Console.WriteLine($"\n\n {count} transactions committed, Execution Time: {ts2 - ts1}.\n\n");
            }
            catch(Exception e)
            {
                Console.WriteLine($"\n\n {e.ToString()}\n\n");
            }
        }      


        private Tuple<uint, uint> getGrains(uint n, Random rand)
        {            
            uint from =(uint) rand.Next( 1, (int)n);
            uint to = (uint)  rand.Next(1, (int)n);
            while (from == to)
            {
                to = (uint)rand.Next(1, (int)n);
            }
            return new Tuple<uint, uint>(from, to);            
        }


        public Task initializeGrain(IClusterClient client)
        {
            DateTime ts1 = DateTime.Now;

            int count = 0;
            //ATM id ranges from 1 to 10;
            for (uint i = 1; i <= numAccounts ; i++)
            {
                IAccountGrain account = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(i));
                count += account.ActivateGrain().Result;
            }

            DateTime ts2 = DateTime.Now;
            Console.WriteLine($"\n\n Initialized {count} actors, cost of time: {ts2 - ts1}.\n\n");

            return Task.CompletedTask;
        }   
    
    }
}
