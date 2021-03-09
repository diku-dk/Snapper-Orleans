using System;
using Orleans;
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
    public class PersistGrain : Grain, IPersistGrain
    {
        private int myID;
        private int index;
        private byte[] buffer;
        private int numWaitLog;
        private FileStream file;
        private int maxNumWaitLog;
        private int maxBufferSize;
        private SemaphoreSlim instanceLock;
        private TaskCompletionSource<bool> waitFlush;

        private long IOcount = 0;

        public override Task OnActivateAsync()
        {
            index = 0;
            numWaitLog = 0;
            maxNumWaitLog = 1;   // 64 is the pipe size
            maxBufferSize = 15000;    // 3 * 64 * 75 = 14400 bytes
            buffer = new byte[maxBufferSize];
            myID = (int)this.GetPrimaryKeyLong();
            instanceLock = new SemaphoreSlim(1);
            waitFlush = new TaskCompletionSource<bool>();
            var fileName = Constants.logPath + myID;
            file = new FileStream(fileName, FileMode.Append, FileAccess.Write);
            return base.OnActivateAsync();
        }

        public async Task<long> GetIOCount()
        {
            return IOcount;
        }

        public async Task SetIOCount()
        {
            maxNumWaitLog = 8;
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
