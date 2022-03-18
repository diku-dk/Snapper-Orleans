using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.Coordinator
{
    public interface IGlobalCoordGrain : IGrainWithIntegerKey
    {
        Task SpawnGlobalCoordGrain();

        Task PassToken(BasicToken token);

        Task<TransactionRegistInfo> NewTransaction();

        Task<TransactionRegistInfo> NewTransaction(List<int> siloList, List<int> coordList);

        //Task AckBatchCompletion(int bid);

        //sTask WaitBatchCommit(int bid);

        Task CheckGC();
    }
}