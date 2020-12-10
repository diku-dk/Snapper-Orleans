using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.Logging
{
    public enum dataFormatType { BINARY, JSON, MSGPACK };
    public enum StorageWrapperType { NOSTORAGE, INMEMORY, FILESYSTEM, DYNAMODB };
    public interface ILoggingProtocol<TState>
    {
        Task HandleBeforePrepareIn2PC(int tid, int coordinatorKey, HashSet<int> grains);

        Task HandleOnPrepareIn2PC(ITransactionalState<TState> state, int tid, int coordinatorKey);

        Task HandleOnCommitIn2PC(ITransactionalState<TState> state, int tid, int coordinatorKey);

        Task HandleOnAbortIn2PC(ITransactionalState<TState> state, int tid, int coordinatorKey);

        Task HandleOnCompleteInDeterministicProtocol(ITransactionalState<TState> state, int tid, int coordinatorKey);

        Task HandleOnPrepareInDeterministicProtocol(int bid, HashSet<int> grains);

        Task HandleOnCommitInDeterministicProtocol(int bid);
    }
}
