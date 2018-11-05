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
        int maxRetries = 10;
        private SemaphoreSlim instanceLock;

        public FileKeyValueStorageWrapper(string basePath)
        {
            this.basePath = basePath;
            instanceLock = new SemaphoreSlim(1);
        }

        Task<byte[]> IKeyValueStorageWrapper.Read(Guid key)
        {
            throw new NotImplementedException();
        }

        async Task IKeyValueStorageWrapper.Write(Guid key, byte[] value)
        {
            var success = false;
            long tries = 0;
            FileStream file = null;
            long fileLength = 0;
            await instanceLock.WaitAsync();
            while (!success && tries <= maxRetries)
            {
                try
                {                    
                    var fileName = basePath + key.ToString();
                    file = new FileStream(fileName, FileMode.Append, FileAccess.Write);
                    fileLength = file.Length;
                    lock (this)
                    {
                        file.Write(value, 0, value.Length);
                        file.Flush();
                    }
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
