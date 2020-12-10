using System;
using Orleans;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Nondeterministic;

namespace Concurrency.Interface
{
    public class LoggingConfiguration
    {
        public bool isLoggingEnabled;
        public dataFormatType dataFormat;
        public StorageWrapperType loggingStorageWrapper;

        public LoggingConfiguration(dataFormatType dataFormat, StorageWrapperType loggingStorageWrapper)
        {
            if (loggingStorageWrapper == StorageWrapperType.NOSTORAGE) isLoggingEnabled = false;
            else isLoggingEnabled = true;
            this.dataFormat = dataFormat;
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
        public string grainClassName;
        public LoggingConfiguration logConfiguration;
        public ConcurrencyConfiguration nonDetCCConfiguration;

        public ExecutionGrainConfiguration(string grainClassName, LoggingConfiguration logConfiguration, ConcurrencyConfiguration nonDetCCConfiguration)
        {
            this.grainClassName = grainClassName;
            this.logConfiguration = logConfiguration;
            this.nonDetCCConfiguration = nonDetCCConfiguration;
        }
    }

    public class CoordinatorGrainConfiguration
    {
        public int batchInterval;
        public int backoffIntervalMSecs;
        public int idleIntervalTillBackOffSecs;
        public int numCoordinators;

        public CoordinatorGrainConfiguration(int batchInterval, int backoffIntervalMSecs, int idleIntervalTillBackOffSecs, int numCoordinators)
        {
            this.batchInterval = batchInterval;
            this.backoffIntervalMSecs = backoffIntervalMSecs;
            this.idleIntervalTillBackOffSecs = idleIntervalTillBackOffSecs;
            this.numCoordinators = numCoordinators;
        }
    }
    public interface IConfigurationManagerGrain : IGrainWithIntegerKey
    {
        Task<Tuple<ExecutionGrainConfiguration, int, int>> GetConfiguration(int grainID);        
        Task UpdateNewConfiguration(CoordinatorGrainConfiguration config);
        Task UpdateNewConfiguration(ExecutionGrainConfiguration config);
    }
}
