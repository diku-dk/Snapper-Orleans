using Utilities;
using System.Threading.Tasks;

namespace Concurrency.Interface.Nondeterministic
{
    public interface INondeterministicTransactionCoordinator : Orleans.IGrainWithGuidKey
    {
        Task<TransactionContext> NewTransaction();
    }
}
