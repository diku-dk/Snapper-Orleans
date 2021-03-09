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
        private IPersistWorker[] persistWorkers;

        public void Init(int numSingleton, int maxNumWaitLog)
        {
            persistWorkers = new IPersistWorker[numSingleton];
            for (int i = 0; i < numSingleton; i++) persistWorkers[i] = new PersistWorker(i, maxNumWaitLog);
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
        private int maxNumWaitLog;
        private int maxBufferSize;
        private SemaphoreSlim instanceLock;
        private TaskCompletionSource<bool> waitFlush;

        private long IOcount = 0;

        public PersistWorker(int myID, int maxNumWaitLog)
        {
            index = 0;
            numWaitLog = 0;
            this.myID = myID;
            maxBufferSize = 15000;    // 3 * 64 * 75 = 14400 bytes
            buffer = new byte[maxBufferSize];
            this.maxNumWaitLog = 1;
            fileName = Constants.logPath + myID;
            instanceLock = new SemaphoreSlim(1);
            waitFlush = new TaskCompletionSource<bool>();
            file = new FileStream(fileName, FileMode.Append, FileAccess.Write);
        }

        public long GetIOCount()
        {
            return IOcount;
        }

        public void SetIOCount()
        {
            maxNumWaitLog = 1;   // maxNumWaitLog
            IOcount = 0;
        }

        public async Task Write(byte[] value)
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
            if (numWaitLog == maxNumWaitLog)
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
