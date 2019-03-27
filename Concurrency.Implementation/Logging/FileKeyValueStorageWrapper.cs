using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using System.IO;
using System.Threading;

namespace Concurrency.Implementation.Logging
{
    class FileKeyValueStorageWrapper : IKeyValueStorageWrapper
    {
        String basePath = "";
        String logName = "";
        int maxRetries = 10;
        private SemaphoreSlim instanceLock;

        public FileKeyValueStorageWrapper(string basePath, String grainType, Guid grainKey)
        {
            this.basePath = basePath;
            this.logName = grainType + grainKey.ToString();
            instanceLock = new SemaphoreSlim(1);
        }

        Task<byte[]> IKeyValueStorageWrapper.Read(byte[] key)
        {
            throw new NotImplementedException();
        }

        async Task IKeyValueStorageWrapper.Write(byte[] key, byte[] value)
        {
            var success = false;
            int tries = 0;
            FileStream file = null;
            long fileLength = 0;
            await instanceLock.WaitAsync();
            while (!success && tries <= maxRetries)
            {
                try
                {
                    var fileName = basePath + logName;
                    file = new FileStream(fileName, FileMode.Append, FileAccess.Write);
                    fileLength = file.Length;
                    var sizeBytes = BitConverter.GetBytes(fileLength);
                    await file.WriteAsync(sizeBytes, 0, sizeBytes.Length);
                    await file.WriteAsync(value, 0, value.Length);
                    await file.FlushAsync();
                    success = true;
                }
                catch (Exception ex)
                {
                    tries++;
                    Console.WriteLine("Exception caught while writing: {0}", ex);
                    file.SetLength(fileLength);
                } finally
                {
                    file.Close();                    
                }
            }
            instanceLock.Release();
        }
    }
}
