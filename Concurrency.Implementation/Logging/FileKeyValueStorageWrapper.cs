using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using Utilities;

namespace Concurrency.Implementation.Logging
{
    class FileKeyValueStorageWrapper : IKeyValueStorageWrapper
    {
        string basePath = Constants.logPath;
        string logName = "";
        int maxRetries = 10;
        private SemaphoreSlim instanceLock;

        public FileKeyValueStorageWrapper(string grainType, int grainID)
        {
            logName = grainType + grainID;
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
                    var sizeBytes = BitConverter.GetBytes(value.Length);
                    await file.WriteAsync(sizeBytes, 0, sizeBytes.Length);
                    await file.WriteAsync(value, 0, value.Length);
                    await file.FlushAsync();
                    //Console.WriteLine($"write {sizeBytes.Length + value.Length} bytes");
                    success = true;
                }
                catch (Exception ex)
                {
                    tries++;
                    Console.WriteLine("Exception caught while writing: {0}", ex);
                    file.SetLength(fileLength);
                }
                finally
                {
                    file.Close();
                }
            }
            instanceLock.Release();
        }
    }
}
