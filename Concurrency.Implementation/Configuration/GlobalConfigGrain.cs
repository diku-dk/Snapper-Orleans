using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Coordinator;

namespace Concurrency.Implementation.Configuration
{
    [GlobalConfigGrainPlacementStrategy]
    public class GlobalConfigGrain : Grain, IGlobalConfigGrain
    {
        private bool tokenEnabled;
        private ILocalConfigGrain[] configGrains;
        private readonly ILoggerGroup loggerGroup;  // this logger group is only accessible within this silo host

        public override Task OnActivateAsync()
        {
            tokenEnabled = false;
            return base.OnActivateAsync();
        }

        public GlobalConfigGrain(ILoggerGroup loggerGroup)   // dependency injection
        {
            this.loggerGroup = loggerGroup;
        }

        public async Task SetIOCount()
        {
            var tasks = new List<Task>();
            for (int i = 0; i < Constants.numSilo; i++) tasks.Add(configGrains[i].SetIOCount());
            await Task.WhenAll(tasks);

            loggerGroup.SetIOCount();
        }

        public async Task<long> GetIOCount()
        {
            var tasks = new List<Task<long>>();
            for (int i = 0; i < Constants.numSilo; i++) tasks.Add(configGrains[i].GetIOCount());
            await Task.WhenAll(tasks);

            var count = loggerGroup.GetIOCount();
            for (int i = 0; i < Constants.numSilo; i++) count += tasks[i].Result;

            return count;
        }
            
        public async Task ConfigGlobalEnv()
        {
            if (Constants.loggingType == LoggingType.LOGGER) loggerGroup.Init(Constants.numGlobalLogger);

            // configure local environment in each silo
            await ConfigLocalEnv();

            // initialize global coordinators
            var tasks = new List<Task>();
            for (int i = 0; i < Constants.numGlobalCoord; i++)
            {
                var coord = GrainFactory.GetGrain<IGlobalCoordGrain>(i);
                tasks.Add(coord.SpawnGlobalCoordGrain());
            }
            await Task.WhenAll(tasks);

            //Inject token to global coordinator 0
            if (tokenEnabled == false)
            {
                var coord0 = GrainFactory.GetGrain<IGlobalCoordGrain>(0);
                BatchToken token = new BatchToken(-1, -1);
                await coord0.PassToken(token);
                tokenEnabled = true;
            }
        }

        private async Task ConfigLocalEnv()
        {
            configGrains = new ILocalConfigGrain[Constants.numSilo];
            var tasks = new List<Task>();
            for (int i = 0; i < Constants.numSilo; i++)
            {
                configGrains[i] = GrainFactory.GetGrain<ILocalConfigGrain>(i);
                tasks.Add(configGrains[i].ConfigLocalEnv());
            }
            await Task.WhenAll(tasks);
        }
    }
}