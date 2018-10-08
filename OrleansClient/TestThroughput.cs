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
using Concurrency.Utilities;

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
        uint n1, n2;
        int N;
        
        public TestThroughput(uint n1, uint n2)
        {
            this.n1 = n1;
            this.n2 = n2;
        }

        public async Task DoTest(IClusterClient client, int nTx, bool isDeterministic)
        {

            //initialize n1 ATM and n2 accounts
            await initializeGrain(client);
            this.N = nTx;

            List<List<uint>> grainsPerTx = new List<List<uint>>();
            //getGrainsNoContention(grainsPerTx);

            Random rand = new Random();
            for (uint i = 0; i < N; i++)
            {
                grainsPerTx.Add(getGrains(n1, n2, rand));
                Console.WriteLine($"Grains in transaction: {grainsPerTx[grainsPerTx.Count-1][0]}, {grainsPerTx[grainsPerTx.Count - 1][1]}, {grainsPerTx[grainsPerTx.Count - 1][2]}\n");
            }

            Console.WriteLine($"\n\n Start running Transactions ....\n\n");

            try
            {
                List<Task<FunctionResult>> tasks = new List<Task<FunctionResult>>();
                DateTime ts1 = DateTime.Now;
                for (int i = 0; i < N; i++)
                {
                    List<uint> grains = grainsPerTx[i];
                    IATMGrain atm = client.GetGrain<IATMGrain>((Helper.convertUInt32ToGuid(grains[0])));
                    IAccountGrain from = client.GetGrain<IAccountGrain>((Helper.convertUInt32ToGuid(grains[1])));
                    IAccountGrain to = client.GetGrain<IAccountGrain>((Helper.convertUInt32ToGuid(grains[2])));

                    if (isDeterministic)
                    {
                        var grainAccessInformation = new Dictionary<Guid, Tuple<String, int>>();
                        Guid fromId = from.GetPrimaryKey();
                        Guid toId = to.GetPrimaryKey();
                        Guid atmId = atm.GetPrimaryKey();
                        grainAccessInformation.Add(fromId, new Tuple<String, int>("AccountTransfer.Grains.AccountGrain", 1));
                        grainAccessInformation.Add(toId, new Tuple<String, int>("AccountTransfer.Grains.AccountGrain", 1));
                        grainAccessInformation.Add(atmId, new Tuple<String, int>("AccountTransfer.Grains.ATMGrain", 1));

                        List<object> input = new List<object> { fromId, toId, 100 };
                        tasks.Add(atm.StartTransaction(grainAccessInformation, "Transfer", new FunctionInput(input)));
                    }
                    else
                    {
                        var args = new TransferInput(grains[1], grains[2], 10);
                        FunctionInput input = new FunctionInput(args);
                        tasks.Add(atm.StartTransaction("Transfer", input));
                    }
                }
                await Task.WhenAll(tasks);
                DateTime ts2 = DateTime.Now;
                int count = 0;
                foreach (var aResultTask in tasks)
                {
                    if (!aResultTask.Result.hasException())
                    {
                        count++;
                    }
                }
                Console.WriteLine($"\n\n {count} transactions committed, Execution Time: {ts2 - ts1}.\n\n");
            }catch(Exception e)
            {
                Console.WriteLine($"\n\n {e.ToString()}\n\n");
            }


        }      


        private List<uint> getGrains(uint n1, uint n2, Random rand)
        {
            List<uint> ret = new List<uint>();
            uint atm = (uint) rand.Next(1, (int)n1);
            uint from =(uint) rand.Next((int)n1 + 1, (int)n1 + (int)n2);
            uint to = (uint)  rand.Next((int)n1 + 1, (int)n1 + (int)n2);
            while (from == to)
            {
                to = (uint)rand.Next((int)n1 + 1, (int)n1 + (int)n2);
            }
            ret.Add(atm);
            ret.Add(from);
            ret.Add(to);

            return ret;
        }

        private void getGrainsNoContention(List<List<uint>> grainsPerTx)
        {
            for(uint i=1; i<=N; i++)
            {
                grainsPerTx.Add(new List<uint>() { i, n1 + i, n1 + (uint)N + i});
            }
        }


        public async Task initializeGrain(IClusterClient client)
        {
            DateTime ts1 = DateTime.Now;

            //ATM id ranges from 1 to 10;
            for (uint i = 1; i <= n1; i++)
            {
                IATMGrain atm = client.GetGrain<IATMGrain>(Helper.convertUInt32ToGuid(i));
                await atm.ActivateGrain();
            }

            //Account id ranges from 11 to 110
            for (uint i = n1+1; i <= n1+n2; i++)
            {
                IAccountGrain account = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(i));
                await account.ActivateGrain();
            }

            DateTime ts2 = DateTime.Now;
            Console.WriteLine($"\n\n Initialization time: {ts2 - ts1}.\n\n");

            return;
        }   
    
    }
}
