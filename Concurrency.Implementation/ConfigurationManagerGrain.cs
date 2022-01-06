using Orleans;
using Utilities;
using Persist.Interfaces;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Implementation
{
    public class ConfigurationManagerGrain : Grain, IConfigurationManagerGrain
    {
        private bool tokenEnabled;
        private readonly IPersistSingletonGroup persistSingletonGroup;

        public override Task OnActivateAsync()
        {
            tokenEnabled = false;
            return base.OnActivateAsync();
        }

        public ConfigurationManagerGrain(IPersistSingletonGroup persistSingletonGroup)
        {
            this.persistSingletonGroup = persistSingletonGroup;
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

        public async Task Initialize()
        {
            if (Constants.loggingType == LoggingType.PERSISTSINGLETON) persistSingletonGroup.Init();

            // initialize coordinators (single silo deployment)
            var tasks = new List<Task>();
            for (int i = 0; i < Constants.numCoordPerSilo; i++)
            {
                var grain = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(i);
                tasks.Add(grain.SpawnCoordinator());
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
        }
    }
}