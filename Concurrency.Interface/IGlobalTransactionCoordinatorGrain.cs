using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Utilities;
using Orleans;
using Orleans.Concurrency;

namespace Concurrency.Interface
{
    struct Message<T>
    {
    }
    public interface IGlobalTransactionCoordinatorGrain : IGrainWithGuidKey
    {
        [AlwaysInterleave]
        Task<Tuple<long, int>> GetStatus();

        /// <summary>
        /// Client calls this function to submit a new deterministic transaction
        /// </summary>
        /// 
        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction(Dictionary<Guid, Tuple<String, int>> grainAccessInformation);

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

        Task SpawnCoordinator(uint myId, uint numOfCoordinators, int batchIntervalMSecs, int backOffIntervalMSecs, int idleIntervalTillBackOffSecs);

        [AlwaysInterleave]
        Task NotifyCommit(int bid);

        [AlwaysInterleave]
        Task<bool> checkBatchCompletion(int bid);

        /// <summary>
        /// Actors call this function to notify coordinator that a transaction has been completed locally. 
        /// </summary>
        [AlwaysInterleave]
        Task AckBatchCompletion(int bid, Guid executor_id);

        [AlwaysInterleave]
        Task<HashSet<int>> GetCompleteAfterSet(Dictionary<Guid, int> grains, Dictionary<Guid, String> names);
       

    }
}
