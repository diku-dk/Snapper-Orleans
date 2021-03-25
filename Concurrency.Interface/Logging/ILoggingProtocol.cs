using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.Logging
{
    public interface ILoggingProtocol<TState>
    {
        Task HandleBeforePrepareIn2PC(int tid, int coordinatorKey, HashSet<Tuple<int, string>> grains);

        Task HandleOnPrepareIn2PC(ITransactionalState<TState> state, int tid, int coordinatorKey);

        Task HandleOnCommitIn2PC(int tid, int coordinatorKey);

        Task HandleOnAbortIn2PC(int tid, int coordinatorKey);

        Task HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, int tid, int coordinatorKey);

        Task HandleOnPrepareInDeterministicProtocol(int bid, HashSet<Tuple<int, string>> grains);

        Task HandleOnCommitInDeterministicProtocol(int bid);
    }
}
