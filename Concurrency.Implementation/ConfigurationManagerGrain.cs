using Orleans;
using Utilities;
using Persist.Interfaces;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Concurrency;
using System;

namespace Concurrency.Implementation
{
    [Reentrant]
    public class ConfigurationManagerGrain : Grain, IConfigurationManagerGrain
    {
        bool tokenEnabled;
        int numCPUPerSilo;
        bool loggingEnabled;
        readonly IPersistSingletonGroup persistSingletonGroup;
        readonly ITPCCManager tpccManager;

        public override Task OnActivateAsync()
        {
            tokenEnabled = false;
            return base.OnActivateAsync();
        }

        public ConfigurationManagerGrain(IPersistSingletonGroup persistSingletonGroup, ITPCCManager tpccManager)
        {
            this.persistSingletonGroup = persistSingletonGroup;
            this.tpccManager = tpccManager;
        }

        public Task SetIOCount()
        {
            persistSingletonGroup.SetIOCount();
            return Task.CompletedTask;
        }

        public Task<long> GetIOCount()
        {
            return Task.FromResult(persistSingletonGroup.GetIOCount());
        }

        public Task<Tuple<int, bool>> GetSiloConfig()
        {
            return Task.FromResult(new Tuple<int ,bool>(numCPUPerSilo, loggingEnabled));
        }

        public async Task<string> Initialize(bool isSnapper, int numCPUPerSilo, bool loggingEnabled)
        {
            var siloPublicIPAddress = Helper.GetPublicIPAddress();
            if (isSnapper == false) return siloPublicIPAddress;

            this.numCPUPerSilo = numCPUPerSilo;
            this.loggingEnabled = loggingEnabled;
            if (loggingEnabled && Constants.loggingType == LoggingType.PERSISTSINGLETON) persistSingletonGroup.Init(numCPUPerSilo);

            // initialize coordinators (single silo deployment)
            var tasks = new List<Task>();
            var numCoordPerSilo = Helper.GetNumCoordPerSilo(numCPUPerSilo);
            for (int i = 0; i < numCoordPerSilo; i++)
            {
                var grain = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(i);
                tasks.Add(grain.SpawnCoordinator(numCPUPerSilo, loggingEnabled));
            }
            await Task.WhenAll(tasks);

            //Inject token to coordinator 0
            if (tokenEnabled == false)
            {
                var coord0 = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(0);
                BatchToken token = new BatchToken(-1, -1);
                await coord0.PassToken(token);
                tokenEnabled = true;
            }

            return siloPublicIPAddress;
        }

        public Task InitializeTPCCManager(int NUM_OrderGrain_PER_D)
        {
            tpccManager.Init(numCPUPerSilo, NUM_OrderGrain_PER_D);
            return Task.CompletedTask;
        }
    }
}