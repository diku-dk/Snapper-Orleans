using System;
using Orleans;
using Utilities;
using Persist.Interfaces;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Nondeterministic;

namespace Concurrency.Implementation
{
    public class ConfigurationManagerGrain : Grain, IConfigurationManagerGrain
    {
        private int numCoord;
        private bool tokenEnabled;
        private ConcurrencyType nonDetCCType;
        private LoggingConfiguration loggingConfig;
        private readonly IPersistSingletonGroup persistSingletonGroup;

        public override Task OnActivateAsync()
        {
            numCoord = 0;
            tokenEnabled = false;
            loggingConfig = null;
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
            if (config.loggingType == LoggingType.PERSISTSINGLETON)
            {
                if (persistSingletonGroup.IsInitialized() == false) persistSingletonGroup.Init(config.numPersistItem, config.loggingBatchSize);
            }
            return Task.CompletedTask;
        }

        public Task UpdateConfiguration(ConcurrencyType nonDetCCType)
        {
            this.nonDetCCType = nonDetCCType;
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
                tasks.Add(grain.SpawnCoordinator(numCoord, config.batchInterval, config.backoffIntervalMSecs, config.idleIntervalTillBackOffSecs, loggingConfig));
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

        public async Task<Tuple<ConcurrencyType, LoggingConfiguration, int>> GetConfiguration()
        {
            if (numCoord == 0) throw new ArgumentException(nameof(numCoord));
            await Task.CompletedTask;
            return new Tuple<ConcurrencyType, LoggingConfiguration, int>(nonDetCCType, loggingConfig, numCoord);
        }
    }
}
