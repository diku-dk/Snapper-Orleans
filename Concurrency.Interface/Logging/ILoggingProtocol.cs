using System.Threading.Tasks;
using System.Collections.Generic;

namespace Concurrency.Interface.Logging
{
    public interface ILoggingProtocol
    {
        Task HandleBeforePrepareIn2PC(int tid, int coordID, HashSet<int> grains);
        Task HandleOnPrepareIn2PC(byte[] state, int tid, int coordID);
        Task HandleOnCommitIn2PC(int tid, int coordID);
        Task HandleOnAbortIn2PC(int tid, int coordID);
        Task HandleOnCompleteInDeterministicProtocol(byte[] state, int tid, int coordID);
        Task HandleOnPrepareInDeterministicProtocol(int bid, HashSet<int> grains);
        Task HandleOnCommitInDeterministicProtocol(int bid);
    }
}