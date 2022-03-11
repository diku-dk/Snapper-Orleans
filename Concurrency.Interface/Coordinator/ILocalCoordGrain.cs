using System;
using Orleans;
using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.Coordinator
{
    public interface ILocalCoordGrain : IGrainWithIntegerKey
    {
        Task SpawnLocalCoordGrain();

        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction(Dictionary<int, Tuple<string, int>> grainAccessInfo);

        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction();

        [AlwaysInterleave]
        Task PassToken(BatchToken token);

        [AlwaysInterleave]
        Task AckBatchCompletion(int bid);

        [AlwaysInterleave]
        Task WaitBatchCommit(int bid);
    }
}