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

    public interface IConfigurationManagerGrain : IGrainWithIntegerKey
    {
        Task UpdateConfiguration(LoggingConfiguration config);
        Task UpdateConfiguration(ConcurrencyType nonDetCCType);
        Task UpdateConfiguration(int numCoord);
        Task<Tuple<ConcurrencyType, LoggingConfiguration, int>> GetConfiguration();

        Task SetIOCount();
        Task<long> GetIOCount();
    }
}