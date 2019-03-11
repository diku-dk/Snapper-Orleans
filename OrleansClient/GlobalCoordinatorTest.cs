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
    /// Latenct tests
    /// </summary>
    public class GlobalCoordinatorTest
    {
        uint numOfCoordinator;
        IClusterClient client;

        public GlobalCoordinatorTest(uint n, IClusterClient client)
        {
            this.numOfCoordinator = n;
            this.client = client;


        }

        public async Task SpawnCoordinator()
        {
            List<Task> tasks = new List<Task>();
            //Spawn coordinators
            for (uint i = 0; i < this.numOfCoordinator; i++)
            {
                IGlobalTransactionCoordinator coordinator = client.GetGrain<IGlobalTransactionCoordinator>(Utilities.Helper.convertUInt32ToGuid(i));
                tasks.Add(coordinator.SpawnCoordinator(i, numOfCoordinator));                   
            }
            await Task.WhenAll(tasks);
        }

        public async Task SingleDetTransaction()
        {
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

            //Deterministic Transactions

            try
            {
                Task t1 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
                Task t2 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
                Task t3 = atm.StartTransaction(grainAccessInformation, "transfer", input);

                await Task.WhenAll(t1, t2, t3);
                //await t1;
            
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n\n {e.ToString()}\n\n");
            }


        }

    }
}
