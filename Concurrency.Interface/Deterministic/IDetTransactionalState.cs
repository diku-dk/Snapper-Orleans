using Utilities;
using System.Threading.Tasks;

namespace Concurrency.Interface.Deterministic
{
    public interface IDetTransactionalState<TState>
    {
        Task<TState> Read(TransactionContext ctx, TState committedState);
        Task<TState> ReadWrite(TransactionContext ctx, TState committedState);
    }
}