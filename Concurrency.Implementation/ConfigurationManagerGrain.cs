using System;
using Orleans;
using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;

namespace Concurrency.Implementation
{
    public class ConfigurationManagerGrain : Grain, IConfigurationManagerGrain
    {
        private int numCoord;
        private bool tokenEnabled;
        private string grainClassName;
        private dataFormatType dataFormat;
        private StorageWrapperType logStorage;
        private ExecutionGrainConfiguration executionGrainGlobalConfig;

        public override Task OnActivateAsync()
        {
            numCoord = 0;
            tokenEnabled = false;
            executionGrainGlobalConfig = null;
            return base.OnActivateAsync();
        }

        async Task<Tuple<ExecutionGrainConfiguration, int, int>> IConfigurationManagerGrain.GetConfiguration(int grainID)
        {
            if (numCoord == 0) throw new ArgumentException(nameof(numCoord));
            if (executionGrainGlobalConfig == null) throw new ArgumentNullException(nameof(executionGrainGlobalConfig));
            var coord = grainID % numCoord;
            await Task.CompletedTask;
            return new Tuple<ExecutionGrainConfiguration, int, int>(executionGrainGlobalConfig, coord, numCoord);
        }

        async Task IConfigurationManagerGrain.UpdateNewConfiguration(CoordinatorGrainConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            numCoord = config.numCoordinators;
            var tasks = new List<Task>();
            for (int i = 0; i < numCoord; i++)
            {
                var grain = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(i);
                tasks.Add(grain.SpawnCoordinator(grainClassName, numCoord, config.batchInterval, config.backoffIntervalMSecs, config.idleIntervalTillBackOffSecs, dataFormat, logStorage));
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

        // called directly by client
        async Task IConfigurationManagerGrain.UpdateNewConfiguration(ExecutionGrainConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            executionGrainGlobalConfig = config;
            grainClassName = config.grainClassName;
            dataFormat = config.logConfiguration.dataFormat;
            logStorage = config.logConfiguration.loggingStorageWrapper;
            await Task.CompletedTask;
        }
    }
}
