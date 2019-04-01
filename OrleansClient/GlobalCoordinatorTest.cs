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
                await coordinator.SpawnCoordinator(i, numOfCoordinator);                  
            }
            //await Task.WhenAll(tasks);
        }

        
        public async Task ConcurrentDetTransaction()
        {
            TestThroughput test = new TestThroughput(10000);
            //for(int i=0; i<10; i++)
                
            await test.DoTest(client, 10000, false);
            //await test.DoTest(client, 100, true);

        }

    }
}
