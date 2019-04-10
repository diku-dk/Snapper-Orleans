using System;
using System.Collections.Generic;
using System.Text;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Nondeterministic;
using System.Threading.Tasks;
using Orleans;

namespace Concurrency.Interface
{
    public class LoggingConfiguration
    {
        public Boolean isLoggingEnabled;
        public StorageWrapperType loggingStorageWrapper;

        public LoggingConfiguration()
        {
            this.isLoggingEnabled = false;
        }

        public LoggingConfiguration(StorageWrapperType loggingStorageWrapper)
        {
            this.isLoggingEnabled = true;
            this.loggingStorageWrapper = loggingStorageWrapper;
        }
    }

    public class ConcurrencyConfiguration
    {
        public ConcurrencyType nonDetConcurrencyManager;

        public ConcurrencyConfiguration(ConcurrencyType nonDetConcurrencyManager)
        {
            this.nonDetConcurrencyManager = nonDetConcurrencyManager;
        }
    }

    public class ExecutionGrainConfiguration
    {
        public LoggingConfiguration logConfiguration;
        public ConcurrencyConfiguration nonDetCCConfiguration;

        public ExecutionGrainConfiguration(LoggingConfiguration logConfiguration, ConcurrencyConfiguration nonDetCCConfiguration)
        {
            this.logConfiguration = logConfiguration;
            this.nonDetCCConfiguration = nonDetCCConfiguration;
        }
    }

    public class CoordinatorGrainConfiguration
    {
        public int batchIntervalMSecs;
        public int backoffIntervalMSecs;
        public uint numCoordinators;

        public CoordinatorGrainConfiguration(int batchIntervalMSecs, int backoffIntervalMSecs, uint numCoordinators)
        {
            this.batchIntervalMSecs = batchIntervalMSecs;
            this.backoffIntervalMSecs = backoffIntervalMSecs;
            this.numCoordinators = numCoordinators;
        }
    }
    public interface IConfigurationManagerGrain : IGrainWithGuidKey
    {
        Task<Tuple<ExecutionGrainConfiguration, uint>> GetConfiguration(String grainClassName, Guid grainId);        
        Task UpdateNewConfiguration(CoordinatorGrainConfiguration config);
        Task UpdateNewConfiguration(ExecutionGrainConfiguration config);
        Task UpdateNewConfiguration(Dictionary<Tuple<String, Guid>, ExecutionGrainConfiguration> grainSpecificConfigs);
    }
}
