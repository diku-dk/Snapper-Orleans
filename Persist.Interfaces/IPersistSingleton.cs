using System.Threading.Tasks;

namespace Persist.Interfaces
{
    public interface IPersistSingletonGroup
    {
        void Init(int numSingleton, int maxNumWaitLog);
        IPersistWorker GetSingleton(int index);
        long GetIOCount();
        void SetIOCount();
        bool IsInitialized();
    }

    public interface IPersistWorker
    {
        Task Write(byte[] value);
        long GetIOCount();
        void SetIOCount();
    }
}
