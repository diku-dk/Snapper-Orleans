using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Utilities;
using Orleans;
using Orleans.Concurrency;

namespace Concurrency.Interface
{
    public interface IDeterministicTransactionCoordinator : IGrainWithGuidKey
    {
        /// <summary>
        /// Start the TC
        /// </summary>
        /// <remarks>
        /// This must be called before any other method.
        /// </remarks>
        Task StartAsync();

        /// <summary>
        /// Stop the TM
        /// </summary>
        Task StopAsync();


        /// <summary>
        /// Emit determinictic transactions received in the current batch;
        /// </summary>
        /// 
        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction(Dictionary<Guid, Tuple<String,int>> grainAccessInformation);

        /// <summary>
        /// Return contetx for a non-determinictic transaction
        /// </summary>
        /// 
        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction();

        /// <summary>
        /// Actors call this function to notify coordinator that a transaction has been completed locally. 
        /// </summary>
        [AlwaysInterleave]
        Task AckBatchCompletion(int bid, Guid executor_id);

        [AlwaysInterleave]
        Task<Boolean> checkBatchCompletion(TransactionContext context);

        Task resetTimer(int batchInterval);

    }
}
