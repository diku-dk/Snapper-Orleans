using System;
using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface
{
    public interface IGlobalTransactionCoordinatorGrain : IGrainWithIntegerKey
    {
        /// <summary>
        /// Use this interface to submit a PACT to a coordinator.
        /// </summary>
        Task<MyTransactionContext> NewTransaction(Dictionary<int, Tuple<string, int>> grainAccessInfo);

        /// <summary>
        /// Use this interface to submit an ACT to a coordinator.
        /// </summary>
        Task<MyTransactionContext> NewTransaction();

        /// <summary>
        /// Use this interface to submit an ACT to a coordinator. This interface is only used for getting breakdown transaction latency.
        /// </summary>
        Task<Tuple<MyTransactionContext, DateTime, DateTime>> NewTransactionAndGetTime();

        /// <summary>
        /// Use this interface to pass token to the neighbor coordinator.
        /// </summary>
        Task PassToken(BatchToken token);

        /// <summary>
        /// Use this interface to initiate the coordinator.
        /// </summary>
        Task SpawnCoordinator(int numCPUPerSilo, bool loggingEnabled);

        /// <summary>
        /// Use this interface to notify the coordinator the completion of a batch on a grain.
        /// </summary>
        Task AckBatchCompletion(int bid);

        /// <summary>
        /// Use this interface to wait for a specific batch to commit so to make sure all batches commit in the order of bid.
        /// </summary>
        Task WaitBatchCommit(int bid);
    }
}