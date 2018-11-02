using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Nondeterministic;
using Utilities;

namespace Concurrency.Implementation.Logging
{
    class Simple2PCLoggingProtocol<TState> : ILoggingProtocol<TState>
    {
        IKeyValueStorageWrapper logStorage;
        Guid grainPrimaryKey;

        public Simple2PCLoggingProtocol(Guid grainPrimaryKey) {
            this.grainPrimaryKey = grainPrimaryKey;
            logStorage = new FileKeyValueStorageWrapper("");
        }

        async Task ILoggingProtocol<TState>.HandleOnAbortIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator)
        {            
            var logRecord = new LogFormat<TState>(LogType.ABORT, onCoordinator, tid);            
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
        }

        async Task ILoggingProtocol<TState>.HandleOnCommitIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator)
        {
            var logRecord = new LogFormat<TState>(LogType.COMMIT, onCoordinator, tid, state.GetCommittedState(tid));
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));
            
        }

        async Task ILoggingProtocol<TState>.HandleOnPrepareIn2PC(ITransactionalState<TState> state, long tid, bool onCoordinator)
        {
            var logRecord = new LogFormat<TState>(LogType.COMMIT, onCoordinator, tid, state.GetPreparedState(tid));
            await logStorage.Write(grainPrimaryKey, Helper.serializeToByteArray<LogFormat<TState>>(logRecord));            
        }
    }
}
