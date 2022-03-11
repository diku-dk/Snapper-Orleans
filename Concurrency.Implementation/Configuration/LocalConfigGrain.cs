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
    [LocalConfigGrainPlacementStrategy]
    public class LocalConfigGrain : Grain, ILocalConfigGrain
    {
        private int siloID;
        private bool tokenEnabled;
        private readonly ILoggerGroup loggerGroup;  // this logger group is only accessible within this silo host

        public override Task OnActivateAsync()
        {
            var myID = (int)this.GetPrimaryKeyLong();
            siloID = LocalCoordGrainPlacementHelper.MapCoordIDToSiloID(myID);
            tokenEnabled = false;
            return base.OnActivateAsync();
        }

        public LocalConfigGrain(ILoggerGroup loggerGroup)   // dependency injection
        {
            this.loggerGroup = loggerGroup;
        }

        public Task SetIOCount()
        {
            loggerGroup.SetIOCount();
            return Task.CompletedTask;
        }

        public Task<long> GetIOCount()
        {
            return Task.FromResult(loggerGroup.GetIOCount());
        }

        public async Task ConfigLocalEnv()
        {
            if (Constants.loggingType == LoggingType.LOGGER) loggerGroup.Init(Constants.numLoggerPerSilo);

            // initialize local coordinators in this silo
            var tasks = new List<Task>();
            var firstCoordID = LocalCoordGrainPlacementHelper.MapSiloIDToFirstCoordID(siloID);
            for (int i = 0; i < Constants.numLocalCoordPerSilo; i++)
            {
                var coordID = i + firstCoordID;
                var coord = GrainFactory.GetGrain<ILocalCoordGrain>(coordID);
                tasks.Add(coord.SpawnLocalCoordGrain());
            }
            await Task.WhenAll(tasks);

            if (tokenEnabled == false)
            {
                // inject token to the first local coordinator in this silo
                var coord0 = GrainFactory.GetGrain<ILocalCoordGrain>(firstCoordID);
                BatchToken token = new BatchToken(-1, -1);
                await coord0.PassToken(token);
                tokenEnabled = true;
            }
        }
    }
}