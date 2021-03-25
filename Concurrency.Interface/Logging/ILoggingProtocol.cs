using System.Threading.Tasks;
using System.Collections.Generic;
using Utilities;
using System;

namespace Concurrency.Interface.Logging
{
    public interface ILoggingProtocol<TState>
    {
        Task HandleBeforePrepareIn2PC(int tid, int coordinatorKey, Dictionary<string, HashSet<int>> grains);

        Task HandleOnPrepareIn2PC(ITransactionalState<TState> state, int tid, int coordinatorKey);

        Task HandleOnCommitIn2PC(int tid, int coordinatorKey);

        Task HandleOnAbortIn2PC(int tid, int coordinatorKey);

        Task HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, int tid, int coordinatorKey);

        Task HandleOnPrepareInDeterministicProtocol(int bid, Dictionary<string, HashSet<int>> grains);

        Task HandleOnCommitInDeterministicProtocol(int bid);

        // if persist PACT input
        Task HandleOnPrepareInDeterministicProtocol(int bid, Dictionary<int, DeterministicBatchSchedule> batchSchedule, Dictionary<int, Tuple<int, object>> inputs);
    }
}
