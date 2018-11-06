using System;
using System.Collections.Generic;
using System.Text;
using Concurrency.Interface.Nondeterministic;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Concurrency.Interface.Logging
{
    public interface ILoggingProtocol<TState>
    {
        Task HandleOnPrepareIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey);

        Task HandleOnCommitIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey);

        Task HandleOnAbortIn2PC(ITransactionalState<TState> state, long tid, Guid coordinatorKey);

        Task HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, long tid, Guid coordinatorKey);

        Task HandleOnPrepareInDeterministicProtocol(long bid);

        Task HandleOnCommitInDeterministicProtocol(long bid);
    }
}
