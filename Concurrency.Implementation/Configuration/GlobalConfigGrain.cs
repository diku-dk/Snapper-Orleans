using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Configuration;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Coordinator;
using System.IO;

namespace Concurrency.Implementation.Configuration
{
    [GlobalConfigGrainPlacementStrategy]
    public class GlobalConfigGrain : Grain, IGlobalConfigGrain
    {
        bool tokenEnabled;
        ILocalConfigGrain[] configGrains;
        readonly ILoggerGroup loggerGroup;  // this logger group is only accessible within this silo host
        readonly ICoordMap coordMap;

        public override Task OnActivateAsync()
        {
            tokenEnabled = false;
            return base.OnActivateAsync();
        }

        public GlobalConfigGrain(ILoggerGroup loggerGroup, ICoordMap coordMap)   // dependency injection
        {
            this.loggerGroup = loggerGroup;
            this.coordMap = coordMap;
            
            // create the log folder if not exists
            if(Directory.Exists(Constants.logPath) == false)
                Directory.CreateDirectory(Constants.logPath);
        }

        public async Task<long> GetIOCount()
        {
            long count = 0;
            if (Constants.multiSilo == false || Constants.hierarchicalCoord)
            {
                // forward the request to all local config grains, which will check GC for local coordinators in that silo

                for (int i = 0; i < Constants.numSilo; i++)
                {
                    configGrains[i] = GrainFactory.GetGrain<ILocalConfigGrain>(i);
                    count += await configGrains[i].GetIOCount();
                }
            }

            if (Constants.multiSilo == false) return count;

            count += loggerGroup.GetIOCount();

            return count;
        }

        public async Task SetIOCount()
        {
            var tasks = new List<Task>();
            if (Constants.multiSilo == false || Constants.hierarchicalCoord)
            {
                // forward the request to all local config grains, which will check GC for local coordinators in that silo

                for (int i = 0; i < Constants.numSilo; i++)
                {
                    configGrains[i] = GrainFactory.GetGrain<ILocalConfigGrain>(i);
                    tasks.Add(configGrains[i].SetIOCount());
                }
            }

            if (Constants.multiSilo == false)
            {
                await Task.WhenAll(tasks);
                return;
            }

            loggerGroup.SetIOCount();

            await Task.WhenAll(tasks);
        }

        public async Task CheckGC()
        {
            var tasks = new List<Task>();
            if (Constants.multiSilo == false || Constants.hierarchicalCoord)
            {
                // forward the request to all local config grains, which will check GC for local coordinators in that silo
                
                for (int i = 0; i < Constants.numSilo; i++)
                {
                    configGrains[i] = GrainFactory.GetGrain<ILocalConfigGrain>(i);
                    tasks.Add(configGrains[i].CheckGC());
                }
            }

            if (Constants.multiSilo == false) 
            {
                await Task.WhenAll(tasks);
                return;
            }

            if (Constants.hierarchicalCoord)
            {
                // check GC for all global coordinators locate in a separate silo
                for (int i = 0; i < Constants.numGlobalCoord; i++)
                {
                    var coord = GrainFactory.GetGrain<IGlobalCoordGrain>(i);
                    tasks.Add(coord.CheckGC());
                }
            }
            else
            {
                // check GC for all local coordinators locate in a separate silo
                for (int i = 0; i < Constants.numGlobalCoord; i++)
                {
                    var coord = GrainFactory.GetGrain<ILocalCoordGrain>(i);
                    tasks.Add(coord.CheckGC());
                }
            }

            await Task.WhenAll(tasks);
        }

        public async Task ConfigGlobalEnv()
        {
            // configure local environment in each silo
            await ConfigLocalEnv();

            if (Constants.multiSilo == false) return;

            if (Constants.loggingType == LoggingType.LOGGER) loggerGroup.Init(Constants.numGlobalLogger, "GlobalLog");

            coordMap.Init(GrainFactory);
            if (Constants.hierarchicalCoord) await ConfigHierarchicalArchitecture();
            else await ConfigSimpleArchitecture();
        }

        async Task ConfigHierarchicalArchitecture()
        {
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
                BasicToken token = new BasicToken();
                await coord0.PassToken(token);
                tokenEnabled = true;
            }
        }

        async Task ConfigSimpleArchitecture()
        {
            // initialize local coordinators (locate in a separate silo)
            var tasks = new List<Task>();
            for (int i = 0; i < Constants.numGlobalCoord; i++)
            {
                var coord = GrainFactory.GetGrain<ILocalCoordGrain>(i);
                tasks.Add(coord.SpawnLocalCoordGrain());
            }
            await Task.WhenAll(tasks);

            //Inject token to local coordinator 0
            if (tokenEnabled == false)
            {
                var coord0 = GrainFactory.GetGrain<ILocalCoordGrain>(0);
                var token = new LocalToken();
                await coord0.PassToken(token);
                tokenEnabled = true;
            }
        }

        async Task ConfigLocalEnv()
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