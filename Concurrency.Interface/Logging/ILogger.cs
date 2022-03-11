using System.Threading.Tasks;

namespace Concurrency.Interface.Logging
{
    public interface ILoggerGroup
    {
        void Init(int numLogger);
        ILogger GetSingleton(int index);
        long GetIOCount();
        void SetIOCount();
    }

    public interface ILogger
    {
        Task Write(byte[] value);
        long GetIOCount();
        void SetIOCount();

        void CleanFile();
    }
}