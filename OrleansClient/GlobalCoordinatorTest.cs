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

    }
}
