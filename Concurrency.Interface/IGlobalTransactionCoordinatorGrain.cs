using System;
using Orleans;
using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface
{
    public interface IGlobalTransactionCoordinatorGrain : IGrainWithIntegerKey
    {
        /// <summary>
        /// Client calls this function to submit a new deterministic transaction
        /// </summary>
        /// 
        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction(Dictionary<int, Tuple<string, int>> grainAccessInfo);

        /// <summary>
        /// Client calls this function to submit a new non-deterministic transaction
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

        Task SpawnCoordinator();

        /// <summary>
        /// Actors call this function to notify coordinator that a transaction has been completed locally. 
        /// </summary>
        [AlwaysInterleave]
        Task AckBatchCompletion(int bid);

        [AlwaysInterleave]
        Task WaitBatchCommit(int bid);
    }
}