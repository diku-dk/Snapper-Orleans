using System.Threading.Tasks;

namespace Concurrency.Interface.Logging
{
    public interface ILoggerGroup
    {
        void Init(int numLogger, string loggerName);
        int GetNumLogger();
        ILogger GetLogger(int index);
        void GetLoggingProtocol(int myID, out ILoggingProtocol log);
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