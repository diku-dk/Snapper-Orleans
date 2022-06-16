using Utilities;
using System.Threading.Tasks;

namespace Concurrency.Interface.Deterministic
{
    public interface IDetTransactionalState<TState>
    {
        Task<TState> Read(MyTransactionContext ctx, TState committedState);
        Task<TState> ReadWrite(MyTransactionContext ctx, TState committedState);
    }
}