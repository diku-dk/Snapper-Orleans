using System;
using Utilities;
using System.IO;
using System.Threading;
using System.Diagnostics;
using Orleans.Concurrency;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;

namespace Concurrency.Implementation.Logging
{
    [Reentrant]
    public class LoggerGroup : ILoggerGroup
    {
        private ILogger[] loggers = null;

        public void Init(int numLogger, string loggerName)
        {
            if (loggers != null) foreach (var logger in loggers) logger.CleanFile();
            else
            {
                loggers = new ILogger[numLogger];
                for (int i = 0; i < numLogger; i++) loggers[i] = new Logger(loggerName, i);
            }
        }

        public ILogger GetLogger(int index)
        {
            return loggers[index];
        }

        public void GetLoggingProtocol(int myID, out ILoggingProtocol log)
        {
            if (Constants.loggingType == LoggingType.LOGGER)
            {
                var loggerID = Helper.MapGrainIDToServiceID(myID, loggers.Length);
                log = new LoggingProtocol(GetType().ToString(), myID, GetLogger(loggerID));
            }
            else if (Constants.loggingType == LoggingType.ONGRAIN)
                log = new LoggingProtocol(GetType().ToString(), myID);
            else log = null;
        }

        public long GetIOCount()
        {
            long count = 0;
            for (int i = 0; i < loggers.Length; i++) count += loggers[i].GetIOCount();
            return count;

        }

        public void SetIOCount()
        {
            for (int i = 0; i < loggers.Length; i++) loggers[i].SetIOCount();
        }
    }

    [Reentrant]
    public class Logger : ILogger
    {
        private int myID;
        private int index;
        private byte[] buffer;
        private int numWaitLog;
        private FileStream file;
        private string fileName;
        private int maxBufferSize;
        private SemaphoreSlim instanceLock;
        private TaskCompletionSource<bool> waitFlush;

        private long IOcount = 0;

        public Logger(string loggerName, int myID)
        {
            index = 0;
            this.myID = myID;
            if (Constants.loggingBatching)
            {
                maxBufferSize = 15000;    // 3 * 64 * 75 = 14400 bytes
                //maxBufferSize = 5 * (int)Math.Pow(10, 5);    // tpcc
                buffer = new byte[maxBufferSize];
                waitFlush = new TaskCompletionSource<bool>();
            }
            fileName = Constants.logPath + loggerName + myID;
            instanceLock = new SemaphoreSlim(1);
            file = new FileStream(fileName, FileMode.Append, FileAccess.Write);
        }

        public void CleanFile()
        {
            file.Close();
            File.Delete(fileName);
            file = new FileStream(fileName, FileMode.Append, FileAccess.Write);
        }

        public long GetIOCount()
        {
            return IOcount;
        }

        public void SetIOCount()
        {
            IOcount = 0;

            file.Close();
            File.Delete(fileName);
            file = new FileStream(fileName, FileMode.Append, FileAccess.Write);
        }

        public async Task Write(byte[] value)
        {
            if (!Constants.loggingBatching)
            {
                await instanceLock.WaitAsync();
                var sizeBytes = BitConverter.GetBytes(value.Length);
                await file.WriteAsync(sizeBytes, 0, sizeBytes.Length);
                await file.WriteAsync(value, 0, value.Length);
                await file.FlushAsync();
                IOcount++;
                instanceLock.Release();
            }
            else
            {
                await instanceLock.WaitAsync();

                // STEP 1: add log to buffer
                var sizeBytes = BitConverter.GetBytes(value.Length);
                Debug.Assert(index + sizeBytes.Length + value.Length <= maxBufferSize);
                Buffer.BlockCopy(sizeBytes, 0, buffer, index, sizeBytes.Length);
                index += sizeBytes.Length;
                Buffer.BlockCopy(value, 0, buffer, index, value.Length);
                index += value.Length;
                numWaitLog++;

                // STEP 2: check if need to flush
                if (numWaitLog == Constants.loggingBatchSize)
                {
                    await Flush();
                    instanceLock.Release();
                }
                else
                {
                    instanceLock.Release();
                    await waitFlush.Task;
                }
            }
        }

        private async Task Flush()
        {
            await file.WriteAsync(buffer, 0, index);
            await file.FlushAsync();

            index = 0;
            IOcount++;
            numWaitLog = 0;
            buffer = new byte[maxBufferSize];

            waitFlush.SetResult(true);
            waitFlush = new TaskCompletionSource<bool>();
        }
    }
}