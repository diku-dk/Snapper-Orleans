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
    /// Latenct tests
    /// </summary>
    public  class TestLatency
    {

        public async Task DoTestDet(IClusterClient client)
        {

            //initialize 10 ATM and 100 accounts
            Dictionary<int, IATMGrain> atmMap = new Dictionary<int, IATMGrain>();
            Dictionary<int, IAccountGrain> accountMap = new Dictionary<int, IAccountGrain>();

            //await initializeGrains(atmMap, accountMap, client);

            int N = 100;
            List<List<int>> grainsPerTx = new List<List<int>>();
            Random rand = new Random();
            for (int i = 0; i < N; i++)
            {
                grainsPerTx.Add(getGrains(rand));
            }

            TimeSpan sumLatency = TimeSpan.FromMilliseconds(0);
            for (int i = 0; i < N; i++)
            {
                List<int> grains = grainsPerTx[i];
                IATMGrain atm = client.GetGrain<IATMGrain>(grains[0]);
                IAccountGrain from = client.GetGrain<IAccountGrain>(grains[1]);
                IAccountGrain to = client.GetGrain<IAccountGrain>(grains[2]);

                var grainAccessInformation = new Dictionary<Guid, Tuple<String,int>>();
                Guid fromId = from.GetPrimaryKey();
                Guid toId = to.GetPrimaryKey();
                Guid atmId = atm.GetPrimaryKey();

                grainAccessInformation.Add(fromId, new Tuple<String,int>("AccountTransfer.Grains.AccountGrain", 1));
                grainAccessInformation.Add(toId, new Tuple<String, int>("AccountTransfer.Grains.AccountGrain", 1));
                grainAccessInformation.Add(atmId, new Tuple<String, int>("AccountTransfer.Grains.ATMGrain", 1));

                List<object> input = new List<object> { fromId, toId, 100 };
                DateTime ts1 = DateTime.Now;
                await atm.StartTransaction(grainAccessInformation, "Transfer", new Concurrency.Utilities.FunctionInput(input));
                DateTime ts2 = DateTime.Now;
                sumLatency += (ts2 - ts1);
                Console.WriteLine($"\n Transaction: {i} latency: {ts2 - ts1}.\n");

            }

            Console.WriteLine($"\n\n Average Latency: {sumLatency/N}.\n\n");

        }       


        private  List<int> getGrains(Random rand)
        {
            List<int> ret = new List<int>();
            int atm = rand.Next(1, 10);
            int from = rand.Next(11, 110);
            int to = rand.Next(11, 110);
            while (from == to)
            {
                to = rand.Next(101, 10100);
            }
            ret.Add(atm);
            ret.Add(from);
            ret.Add(to);

            return ret;
        }

        private  async Task initializeGrains(IClusterClient client)
        {
            DateTime ts1 = DateTime.Now;

            //ATM id ranges from 1 to 10;
            for (int i = 1; i <= 10; i++)
            {
                IATMGrain atm = client.GetGrain<IATMGrain>(i);
                await atm.ActivateGrain();
            }

            //Account id ranges from 11 to 110
            for (int i = 11; i <= 110; i++)
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
