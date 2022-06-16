using Orleans;
using System;
using System.Threading.Tasks;

namespace Concurrency.Interface
{
    public interface IConfigurationManagerGrain : IGrainWithIntegerKey
    {
        /// <summary>
        /// Use this interface to initiate silo.
        /// </summary>
        Task<string> Initialize(bool isSnapper, int numCPUPerSilo, bool loggingEnabled);

        /// <summary>
        /// Use this interface to initiate TPCC configuration.
        /// </summary>
        Task InitializeTPCCManager(int NUM_OrderGrain_PER_D);

        /// <summary>
        /// Use this interface to get silo configuration.
        /// </summary>
        Task<Tuple<int, bool>> GetSiloConfig();

        Task SetIOCount();
        Task<long> GetIOCount();
    }
}