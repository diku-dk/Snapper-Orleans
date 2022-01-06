using System;
using Utilities;
using System.IO;
using System.Threading;
using Persist.Interfaces;
using System.Diagnostics;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace Persist.Grains
{
    [Reentrant]
    public class PersistSingletonGroup : IPersistSingletonGroup
    {
        private IPersistWorker[] persistWorkers = null;

        public void Init()
        {
            if (persistWorkers != null) foreach (var worker in persistWorkers) worker.CleanFile();
            else
            {
                persistWorkers = new IPersistWorker[Constants.numPersistItemPerSilo];
                for (int i = 0; i < Constants.numPersistItemPerSilo; i++)
                    persistWorkers[i] = new PersistWorker(i);
            }
        }

        public IPersistWorker GetSingleton(int index)
        {
            return persistWorkers[index];
        }

        public long GetIOCount()
        {
            long count = 0;
            for (int i = 0; i < persistWorkers.Length; i++) count += persistWorkers[i].GetIOCount();
            return count;

        }

        public void SetIOCount()
        {
            for (int i = 0; i < persistWorkers.Length; i++) persistWorkers[i].SetIOCount();
        }
    }

    [Reentrant]
    public class PersistWorker : IPersistWorker
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

        public PersistWorker(int myID)
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
            fileName = Constants.logPath + myID;
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
