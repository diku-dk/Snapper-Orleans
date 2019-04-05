using System;
using System.Collections.Generic;
using System.Text;
using Concurrency.Interface;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Concurrency.Interface.Logging
{
    public enum StorageWrapperType { FILESYSTEM, DYNAMODB };
    public interface ILoggingProtocol<TState>
    {
        Task HandleBeforePrepareIn2PC(int tid, Guid coordinatorKey, HashSet<Guid> grains);

        Task HandleOnPrepareIn2PC(ITransactionalState<TState> state, int tid, Guid coordinatorKey);

        Task HandleOnCommitIn2PC(ITransactionalState<TState> state, int tid, Guid coordinatorKey);

        Task HandleOnAbortIn2PC(ITransactionalState<TState> state, int tid, Guid coordinatorKey);

        Task HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, int tid, Guid coordinatorKey);

        Task HandleOnPrepareInDeterministicProtocol(int bid, HashSet<Guid> grains);

        Task HandleOnCommitInDeterministicProtocol(int bid);
    }
}
