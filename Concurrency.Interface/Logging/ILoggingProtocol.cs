using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.Logging
{
    public interface ILoggingProtocol
    {
        Task HandleBeforePrepareIn2PC(long tid, int coordID, HashSet<int> grains);
        Task HandleOnPrepareIn2PC(byte[] state, long tid, int coordID);
        Task HandleOnCommitIn2PC(long tid, int coordID);
        Task HandleOnAbortIn2PC(long tid, int coordID);
        Task HandleOnCompleteInDeterministicProtocol(byte[] state, long tid, int coordID);
        Task HandleOnPrepareInDeterministicProtocol(long bid, HashSet<int> grains);
        Task HandleOnCommitInDeterministicProtocol(long bid);
    }
}