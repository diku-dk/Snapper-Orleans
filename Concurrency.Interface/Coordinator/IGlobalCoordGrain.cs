using System;
using Orleans;
using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.Coordinator
{
    public interface IGlobalCoordGrain : IGrainWithIntegerKey
    {
        Task SpawnGlobalCoordGrain();

        [AlwaysInterleave]
        Task PassToken(BatchToken token);
        /*
        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction(Dictionary<int, Tuple<string, int>> grainAccessInfo);

        [AlwaysInterleave]
        Task<TransactionContext> NewTransaction();

        [AlwaysInterleave]
        Task AckBatchCompletion(int bid);

        [AlwaysInterleave]
        Task WaitBatchCommit(int bid);*/
    }
}