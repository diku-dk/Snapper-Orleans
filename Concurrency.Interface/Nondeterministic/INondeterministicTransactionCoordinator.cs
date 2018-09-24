using Concurrency.Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Concurrency.Interface.Nondeterministic
{
    public interface INondeterministicTransactionCoordinator
    {
        
        Task<TransactionContext> NewTransaction();
    }
}
