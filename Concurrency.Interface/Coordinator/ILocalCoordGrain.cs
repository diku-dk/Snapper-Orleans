using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace Concurrency.Interface.Coordinator
{
    public interface ILocalCoordGrain : IGrainWithIntegerKey
    {
        Task SpawnLocalCoordGrain();

        Task<TransactionRegistInfo> NewTransaction(List<int> grainAccessInfo, List<string> grainClassName);

        Task<TransactionRegistInfo> NewTransaction();

        Task PassToken(LocalToken token);

        Task AckBatchCompletion(int bid);

        Task WaitBatchCommit(int bid);

        // for global transactions (hierarchical architecture)
        Task<TransactionRegistInfo> NewGlobalTransaction(int globalTid, List<int> grainAccessInfo);
        Task ReceiveBatchSchedule(SubBatch batch);

        Task CheckGC();
    }
}