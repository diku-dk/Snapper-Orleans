using System;
using Orleans;
using Utilities;
using System.Threading.Tasks;
using Concurrency.Interface.Nondeterministic;

namespace Concurrency.Interface
{
    public class LoggingConfiguration
    {
        public bool batching;
        public int numPersistItem;
        public int loggingBatchSize;
        public LoggingType loggingType;
        public StorageType storageType;
        public SerializerType serializerType;

        public LoggingConfiguration(LoggingType loggingType, StorageType storageType, SerializerType serializerType, int numPersistItem, int loggingBatchSize, bool batching)
        {
            this.loggingType = loggingType;
            if (loggingType != LoggingType.NOLOGGING)
            {
                this.storageType = storageType;
                this.serializerType = serializerType;
                if (loggingType != LoggingType.ONGRAIN)
                {
                    this.batching = batching;
                    this.numPersistItem = numPersistItem;
                    this.loggingBatchSize = loggingBatchSize;
                }
            }
        }
    }

    public class ExecutionGrainConfiguration
    {
        public ConcurrencyType nonDetCCType;

        public ExecutionGrainConfiguration(ConcurrencyType nonDetCCType)
        {
            this.nonDetCCType = nonDetCCType;
        }
    }

    public class CoordinatorGrainConfiguration
    {
        public int batchInterval;
        public int numCoordinators;
        public int backoffIntervalMSecs;
        public int idleIntervalTillBackOffSecs;
        
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
        Task UpdateConfiguration(LoggingConfiguration config);
        Task UpdateConfiguration(ConcurrencyType nonDetCCType);
        Task UpdateConfiguration(CoordinatorGrainConfiguration config);
        Task<Tuple<ConcurrencyType, LoggingConfiguration, int>> GetConfiguration();

        Task SetIOCount();
        Task<long> GetIOCount();
    }
}
