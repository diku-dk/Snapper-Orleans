using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Interface.Nondeterministic
{
    public interface ITransactionalState<TState>
    {

        Task<TState> Read(long tid);

        Task<TState> ReadWrite(long tid);

        Task Write(long tid);

        Task<bool> Prepare(long tid);

        Task Commit(long tid);

        Task Abort(long tid);

    }
}
