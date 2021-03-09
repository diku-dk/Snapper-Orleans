using System;
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
        private int numCoord;
        private bool tokenEnabled;
        private string grainClassName;
        private LoggingConfiguration loggingConfig;
        private ExecutionGrainConfiguration exeConfig;
        private readonly IPersistSingletonGroup persistSingletonGroup;

        public override Task OnActivateAsync()
        {
            numCoord = 0;
            tokenEnabled = false;
            loggingConfig = null;
            exeConfig = null;
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

        public Task UpdateConfiguration(LoggingConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            loggingConfig = config;
            persistSingletonGroup.Init(config.numPersistItem, config.loggingBatchSize);
            return Task.CompletedTask;
        }

        public Task UpdateConfiguration(ExecutionGrainConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            exeConfig = config;
            grainClassName = config.grainClassName;
            return Task.CompletedTask;
        }

        public async Task UpdateConfiguration(CoordinatorGrainConfiguration config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            numCoord = config.numCoordinators;
            var tasks = new List<Task>();
            for (int i = 0; i < numCoord; i++)
            {
                var grain = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(i);
                tasks.Add(grain.SpawnCoordinator(grainClassName, numCoord, config.batchInterval, config.backoffIntervalMSecs, config.idleIntervalTillBackOffSecs, loggingConfig));
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

        public async Task<Tuple<ExecutionGrainConfiguration, LoggingConfiguration, int>> GetConfiguration()
        {
            if (numCoord == 0) throw new ArgumentException(nameof(numCoord));
            if (exeConfig == null) throw new ArgumentNullException(nameof(exeConfig));
            await Task.CompletedTask;
            return new Tuple<ExecutionGrainConfiguration, LoggingConfiguration, int>(exeConfig, loggingConfig, numCoord);
        }
    }
}
