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
using Concurrency.Interface.Nondeterministic;
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
            this.numOfCoordinator = 5;
        }

        public async Task SpawnCoordinator()
        {
            //Spawn coordinators

            var configGrain = client.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
            var exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(ConcurrencyType.TIMESTAMP));
            var coordConfig = new CoordinatorGrainConfiguration(1000, 1000, 5);
            await configGrain.UpdateNewConfiguration(exeConfig);
            await configGrain.UpdateNewConfiguration(coordConfig);
            //await Task.WhenAll(tasks);
        }

        
        public async Task ConcurrentDetTransaction()
        {

            TestThroughput test = new TestThroughput(100);
            //for(int i=0; i<10; i++)
            List<Task> tasks = new List<Task>();
            await test.DoTest(client, 1000, false);
            await test.DoTest(client, 1000, true);
        }

    }
}
