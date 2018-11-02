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
        Task HandleOnPrepareIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator);

        Task HandleOnCommitIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator);

        Task HandleOnAbortIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator);
    }
}
