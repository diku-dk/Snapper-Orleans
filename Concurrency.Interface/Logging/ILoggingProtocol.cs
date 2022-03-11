using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.TransactionExecution;

namespace Concurrency.Interface.Logging
{
    public interface ILoggingProtocol<TState>
    {
        Task HandleBeforePrepareIn2PC(int tid, int coordID, HashSet<int> grains);
        Task HandleOnPrepareIn2PC(ITransactionalState<TState> state, int tid, int coordID);
        Task HandleOnCommitIn2PC(int tid, int coordID);
        Task HandleOnAbortIn2PC(int tid, int coordID);
        Task HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, int tid, int coordID);
        Task HandleOnPrepareInDeterministicProtocol(int bid, HashSet<int> grains);
        Task HandleOnCommitInDeterministicProtocol(int bid);
    }
}