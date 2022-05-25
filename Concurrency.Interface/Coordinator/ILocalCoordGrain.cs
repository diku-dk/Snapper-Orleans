using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;

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

        Task AckGlobalBatchCommit(int globalBid);

        // for global transactions (hierarchical architecture)
        Task<TransactionRegistInfo> NewGlobalTransaction(int globalBid, int globalTid, List<int> grainAccessInfo, List<string> grainClassName);
        Task ReceiveBatchSchedule(SubBatch batch);

        Task CheckGC();
    }
}