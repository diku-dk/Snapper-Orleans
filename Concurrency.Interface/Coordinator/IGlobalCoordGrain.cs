using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace Concurrency.Interface.Coordinator
{
    public interface IGlobalCoordGrain : IGrainWithIntegerKey
    {
        Task SpawnGlobalCoordGrain();

        Task PassToken(BasicToken token);

        Task<TransactionRegistInfo> NewTransaction();

        Task<Tuple<TransactionRegistInfo, Dictionary<int, int>>> NewTransaction(List<int> siloList);

        Task AckBatchCompletion(int bid);

        Task WaitBatchCommit(int bid);

        Task CheckGC();
    }
}