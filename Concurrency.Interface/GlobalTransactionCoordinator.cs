using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Utilities;
using Orleans;
using Orleans.Concurrency;

namespace Concurrency.Interface
{
    interface GlobalTransactionCoordinator
    {

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
        Task ScheduleBatchEmit();

        /// <summary>
        /// Transaction coordinator of deterministic batches calls this function to notify the completion of a deterministic batch
        /// </summary>
        [AlwaysInterleave]
        Task AckTransactionCompletion(int bid, Guid executor_id);







    }
}
