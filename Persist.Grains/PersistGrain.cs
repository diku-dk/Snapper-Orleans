using System;
using Orleans;
using Utilities;
using System.IO;
using System.Threading;
using Persist.Interfaces;
using System.Threading.Tasks;

namespace Persist.Grains
{
    public class PersistGrain : Grain, IPersistGrain
    {
        private int myID;
        private int index;
        private byte[] buffer;
        private string fileName;
        private int maxBufferSize;
        private SemaphoreSlim instanceLock;
        private TaskCompletionSource<bool> waitFlush;

        public PersistGrain()
        { 
        }

        public override Task OnActivateAsync()
        {
            index = 0;
            maxBufferSize = 1000;
            buffer = new byte[maxBufferSize];
            myID = (int)this.GetPrimaryKeyLong();
            fileName = Constants.logPath + myID;
            instanceLock = new SemaphoreSlim(1);
            waitFlush = new TaskCompletionSource<bool>();
            return base.OnActivateAsync();
        }

        public async Task Write(byte[] value)
        {
            await instanceLock.WaitAsync(); 
            var sizeBytes = BitConverter.GetBytes(value.Length);
            if (index + sizeBytes.Length + value.Length > maxBufferSize) Flush();
            Buffer.BlockCopy(sizeBytes, 0, buffer, index, sizeBytes.Length);
            index += sizeBytes.Length;
            Buffer.BlockCopy(value, 0, buffer, index, value.Length);
            index += value.Length;
            instanceLock.Release();
            await waitFlush.Task;
        }

        private void Flush()
        {
            using (var file = new FileStream(fileName, FileMode.Append, FileAccess.Write))
            {
                file.Write(buffer, 0, index);
            }
            buffer = new byte[maxBufferSize];
            index = 0;
            waitFlush.SetResult(true);
            waitFlush = new TaskCompletionSource<bool>();
        }
    }
}
