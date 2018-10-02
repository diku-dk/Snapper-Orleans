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
        int n1, n2, N;
        
        public TestThroughput(int n1, int n2)
        {
            this.n1 = n1;
            this.n2 = n2;
        }

        public async Task DoTestDet(IClusterClient client, int nTx)
        {

            //initialize n1 ATM and n2 accounts
            //await initializeGrains(1000, 10000, client);
            this.N = nTx;

            List<List<int>> grainsPerTx = new List<List<int>>();
            //getGrainsNoContention(grainsPerTx);

            Random rand = new Random();
            for (int i = 0; i < N; i++)
            {
                grainsPerTx.Add(getGrains(n1, n2, rand));
            }

            Console.WriteLine($"\n\n Start running Transactions ....\n\n");

            List<Task> tasks = new List<Task>();
            DateTime ts1 = DateTime.Now;
            for (int i = 0; i < N; i++)
            {
                List<int> grains = grainsPerTx[i];
                IATMGrain atm = client.GetGrain<IATMGrain>(grains[0]);
                IAccountGrain from = client.GetGrain<IAccountGrain>(grains[1]);
                IAccountGrain to = client.GetGrain<IAccountGrain>(grains[2]);

                Dictionary<Guid, int> grainToAccessTimes = new Dictionary<Guid, int>();
                Guid fromId = from.GetPrimaryKey();
                Guid toId = to.GetPrimaryKey();
                Guid atmId = atm.GetPrimaryKey();

                grainToAccessTimes.Add(fromId, 1);
                grainToAccessTimes.Add(toId, 1);
                grainToAccessTimes.Add(atmId, 1);

                Dictionary<Guid, String> grainClassName = new Dictionary<Guid, String>();
                grainClassName.Add(fromId, "AccountTransfer.Grains.AccountGrain");
                grainClassName.Add(toId, "AccountTransfer.Grains.AccountGrain");
                grainClassName.Add(atmId, "AccountTransfer.Grains.ATMGrain");

                List<object> input = new List<object> { fromId, toId, 100 };


                tasks.Add(atm.StartTransaction(grainToAccessTimes, grainClassName, "Transfer", new Concurrency.Utilities.FunctionInput(input)));

            }
            await Task.WhenAll(tasks);
            DateTime ts2 = DateTime.Now;
            Console.WriteLine($"\n\n Execution Time: {ts2 - ts1}.\n\n");

        }      


        private List<int> getGrains(int n1, int n2, Random rand)
        {
            List<int> ret = new List<int>();
            int atm = rand.Next(1, n1);
            int from = rand.Next(n1 + 1, n1 + n2);
            int to = rand.Next(n1 + 1, n1 + n2);
            while (from == to)
            {
                to = rand.Next(n1 + 1, n1 + n2);
            }
            ret.Add(atm);
            ret.Add(from);
            ret.Add(to);

            return ret;
        }

        private void getGrainsNoContention(List<List<int>> grainsPerTx)
        {
            for(int i=1; i<=N; i++)
            {
                grainsPerTx.Add(new List<int>() { i, n1 + i, n1 + N + i});
            }
        }


        public async Task initializeGrainDet(IClusterClient client)
        {
            DateTime ts1 = DateTime.Now;

            //ATM id ranges from 1 to 10;
            for (int i = 1; i <= n1; i++)
            {
                IATMGrain atm = client.GetGrain<IATMGrain>(i);
                await atm.ActivateGrain();
            }

            //Account id ranges from 11 to 110
            for (int i = n1+1; i <= n1+n2; i++)
            {
                IAccountGrain account = client.GetGrain<IAccountGrain>(i);
                await account.ActivateGrain();
            }

            DateTime ts2 = DateTime.Now;
            Console.WriteLine($"\n\n Initialization time: {ts2 - ts1}.\n\n");

            return;
        }   
    
    }
}
