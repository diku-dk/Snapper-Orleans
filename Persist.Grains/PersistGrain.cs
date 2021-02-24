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

        private long IOcount = 0;

        private IDisposable disposable;

        public PersistGrain()
        { 
        }

        public override Task OnActivateAsync()
        {
            index = 0;
            maxBufferSize = 2000;
            buffer = new byte[maxBufferSize];
            myID = (int)this.GetPrimaryKeyLong();
            fileName = Constants.logPath + myID;
            instanceLock = new SemaphoreSlim(1);
            waitFlush = new TaskCompletionSource<bool>();
            disposable = RegisterTimer(TryFlush, null, TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(10));
            return base.OnActivateAsync();
        }

        public async Task<long> GetIOCount()
        {
            return IOcount;
        }

        public async Task SetIOCount()
        {
            IOcount = 0;
        }

        public async Task Write(byte[] value)
        {
            await instanceLock.WaitAsync(); 
            var sizeBytes = BitConverter.GetBytes(value.Length);
            if (index + sizeBytes.Length + value.Length > maxBufferSize)
            {
                //Console.WriteLine($"grain {myID} flush {index} bytes because buffer is full. ");
                await Flush();
            } 
            Buffer.BlockCopy(sizeBytes, 0, buffer, index, sizeBytes.Length);
            index += sizeBytes.Length;
            Buffer.BlockCopy(value, 0, buffer, index, value.Length);
            index += value.Length;
            instanceLock.Release();
            await waitFlush.Task;
            //Console.WriteLine($"log size = {sizeBytes.Length + value.Length}");
        }

        private async Task TryFlush(object obj)
        {
            //Console.WriteLine($"grain {myID} flush {index} bytes because of timer. ");
            await instanceLock.WaitAsync();
            if (index > 0) await Flush();
            instanceLock.Release();
        }

        private async Task Flush()
        {
            using (var file = new FileStream(fileName, FileMode.Append, FileAccess.Write))
            {
                await file.WriteAsync(buffer, 0, index);
                await file.FlushAsync();
            }
            buffer = new byte[maxBufferSize];
            index = 0;
            waitFlush.SetResult(true);
            waitFlush = new TaskCompletionSource<bool>();
            IOcount++;
        }
    }
}
