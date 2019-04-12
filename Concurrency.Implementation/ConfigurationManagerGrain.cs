﻿using System;
using System.Collections.Generic;
using System.Text;
using Orleans;
using Concurrency.Interface;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation
{
    public class ConfigurationManagerGrain : Grain, IConfigurationManagerGrain
    {        
        Dictionary<Tuple<String, Guid>, ITransactionExecutionGrain> grainIndex;
        Dictionary<Tuple<String, Guid>, ExecutionGrainConfiguration> executionGrainSpecificConfigs;
        ExecutionGrainConfiguration executionGrainGlobalConfig;
        CoordinatorGrainConfiguration coordinatorGrainGlobalConfig;
        uint nextCoordinatorId = 0;

        public override Task OnActivateAsync()
        {            
            executionGrainGlobalConfig = null;
            coordinatorGrainGlobalConfig = null;
            grainIndex = new Dictionary<Tuple<string, Guid>, ITransactionExecutionGrain>();
            executionGrainSpecificConfigs = new Dictionary<Tuple<string, Guid>, ExecutionGrainConfiguration>();
            return base.OnActivateAsync();
        }
        
        async Task<Tuple<ExecutionGrainConfiguration, uint>> IConfigurationManagerGrain.GetConfiguration(string grainClassName, Guid grainId)
        {
            var tuple = new Tuple<string, Guid>(grainClassName, grainId);
            if(!grainIndex.ContainsKey(tuple))
            {
                var grain = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grainId, grainClassName);
                grainIndex.Add(tuple, grain);
            }
            if(coordinatorGrainGlobalConfig == null)
            {
                throw new Exception("No information about coordinators has been registered");
            }
            nextCoordinatorId = (nextCoordinatorId + 1) % coordinatorGrainGlobalConfig.numCoordinators;
            return (executionGrainSpecificConfigs.ContainsKey(tuple)) ? new Tuple<ExecutionGrainConfiguration, uint>(executionGrainSpecificConfigs[tuple], nextCoordinatorId) : new Tuple<ExecutionGrainConfiguration, uint>(executionGrainGlobalConfig, nextCoordinatorId);
        }

        async Task IConfigurationManagerGrain.UpdateNewConfiguration(CoordinatorGrainConfiguration config)
        {
            if (config == null || coordinatorGrainGlobalConfig != null)
                return;

            if (coordinatorGrainGlobalConfig == null)
            {
                coordinatorGrainGlobalConfig = config;
                //Only support one coordinator configuration injection for now
                var tasks = new List<Task>();
                for(uint i=0;i<config.numCoordinators;i++)
                {
                    var grain = this.GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(Helper.convertUInt32ToGuid(i));
                    tasks.Add(grain.SpawnCoordinator(i, config.numCoordinators, config.batchIntervalMSecs, config.backoffIntervalMSecs, config.idleIntervalTillBackOffSecs));
                }
                await Task.WhenAll(tasks);
                //Inject token to coordinator 0
                var coord0 = this.GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(Helper.convertUInt32ToGuid(0));
                BatchToken token = new BatchToken(-1, -1);
                await coord0.PassToken(token);
            } else
            {
                //Only support one coordinator configuration injection for now
                throw new NotImplementedException("Cannot support multiple global coordinator configuration injection");
            }
        }

        async Task IConfigurationManagerGrain.UpdateNewConfiguration(ExecutionGrainConfiguration config)
        {
            if(config == null || executionGrainGlobalConfig != null)
            {
                return;
            }
            //Support only single config changes for now
            if (this.executionGrainGlobalConfig == null)
            {
                this.executionGrainGlobalConfig = config;
            }
        }

        async Task IConfigurationManagerGrain.UpdateNewConfiguration(Dictionary<Tuple<string, Guid>, ExecutionGrainConfiguration> grainSpecificConfigs)
        {
            //Insert or update the existing configuration
            foreach(var entry in grainSpecificConfigs)
            {
                executionGrainSpecificConfigs[entry.Key] = entry.Value;
            }
        }
    }
}