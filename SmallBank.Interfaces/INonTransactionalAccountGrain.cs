using Utilities;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{
    public interface INonTransactionalAccountGrain : Orleans.IGrainWithIntegerKey
    {
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
    }
}
