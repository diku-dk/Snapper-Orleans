using System;
using System.Collections.Generic;
using System.Text;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Nondeterministic;
using System.Threading.Tasks;

namespace Concurrency.Interface
{
    public class LoggingConfiguration
    {
        public Boolean isLoggingEnabled;
        public StorageWrapperType loggingStorageWrapper;
    }

    public class ConcurrencyConfiguration
    {
        public ConcurrencyType nonDetConcurrencyManager;
    }

    public class Configuration
    {
        public LoggingConfiguration logConfiguration;
        public ConcurrencyConfiguration nonDetCCConfiguration;
        public int numCoordinators;
    }
    public interface IConfigurationManager
    {
        Task<Configuration> GetConfiguration(String grainClassName, Guid grainId);

        Task<Configuration> GetConfiguration();

        Task SpawnCoordinators(int numCoordinators);

        Task UpdateNewConfiguration(Configuration config);

        Task UpdateNewConfiguration(Dictionary<Tuple<String, Guid>, Configuration> grainSpecificConfigs);

        Task IncreaseCoordinators(int numCoordinators);

        Task DecreaseCoordinators(int numCoordinators);

    }
}
