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

namespace OrleansClient
{
    /// <summary>
    /// Latenct tests
    /// </summary>
    public class GlobalCoordinatorTest
    {
        uint numOfCoordinator;
        IClusterClient client;

        public GlobalCoordinatorTest(IClusterClient client)
        {
            this.client = client;
        }

        public async Task SpawnCoordinator()
        {
            //Spawn coordinators
            for (uint i = 0; i < this.numOfCoordinator; i++)
            {
                IGlobalTransactionCoordinator coordinator = client.GetGrain<IGlobalTransactionCoordinator>(Utilities.Helper.convertUInt32ToGuid(i));
                await coordinator.SpawnCoordinator(i, numOfCoordinator);                  
            }
            IGlobalTransactionCoordinator coord_0 = client.GetGrain<IGlobalTransactionCoordinator>(Utilities.Helper.convertUInt32ToGuid(0));
            BatchToken token = new BatchToken(-1, -1);
            await coord_0.PassToken(token);

            //await Task.WhenAll(tasks);
        }

        
        public async Task ConcurrentDetTransaction()
        {
            this.numOfCoordinator = 5;
            await this.SpawnCoordinator();
            TestThroughput test = new TestThroughput(100);
            //for(int i=0; i<10; i++)
            List<Task> tasks = new List<Task>();
            Thread.Sleep(5000);
            await test.DoTest(client, 100, true);
            await test.DoTest(client, 100, false);
            await test.DoTest(client, 100, true);
            await test.DoTest(client, 100, false);
            await test.DoTest(client, 100, true);

        }

    }
}
