using System;
using Orleans;
using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;

namespace Concurrency.Interface
{
    struct Message<T>
    {
    }
    public interface IGlobalTransactionCoordinatorGrain : IGrainWithIntegerKey
    {
        /// <summary>
        /// Client calls this function to submit a new deterministic transaction
        /// </summary>
        /// 
        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction(Dictionary<int, int> grainAccessInformation);

        /// <summary>
        /// Client calls this function to submit a new non-deterministic transaction
        /// TODO: should it be interleaved?
        /// </summary>
        /// 
        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction();

        /// <summary>
        /// Coordinators call this function to pass the emit token to its neighbour
        /// Parameters:
        ///     LastEmittedBatchID
        ///     LastCommittedBatchID
        ///     BatchDependencyPerActor: <Actor_ID : List of batch dependency>
        ///     BatchDependency: <Batch_ID, (Transitive) batch dependency list>    
        /// </summary>
        /// 
        [AlwaysInterleave]
        Task PassToken(BatchToken token);

        Task SpawnCoordinator(string grainClassName, int numOfCoordinators, int batchInterval, int backOffIntervalMSecs, int idleIntervalTillBackOffSecs, LoggingConfiguration loggingConfig);

        /// <summary>
        /// Actors call this function to notify coordinator that a transaction has been completed locally. 
        /// </summary>
        [AlwaysInterleave]
        Task AckBatchCompletion(int bid);

        [AlwaysInterleave]
        Task WaitBatchCommit(int bid);
    }
}
