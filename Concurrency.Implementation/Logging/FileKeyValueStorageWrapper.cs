using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using System.IO;

namespace Concurrency.Implementation.Logging
{
    class FileKeyValueStorageWrapper : IKeyValueStorageWrapper
    {
        String basePath = "";

        public FileKeyValueStorageWrapper(string basePath)
        {
            this.basePath = basePath;
        }

        Task<byte[]> IKeyValueStorageWrapper.Read(Guid key)
        {
            throw new NotImplementedException();
        }

        async Task IKeyValueStorageWrapper.Write(Guid key, byte[] value)
        {
            var success = false;
            FileStream file = null;
            long fileLength = 0;
            while (!success)
            {
                try
                {
                    var fileName = basePath + key.ToString();
                    file = new FileStream(fileName, FileMode.Append, FileAccess.ReadWrite);
                    fileLength = file.Length;
                    await file.WriteAsync(value, 0, value.Length);
                    await file.FlushAsync();
                    success = true;
                }
                catch (Exception ex)
                {                    
                    Console.WriteLine("Exception caught while writing: {0}", ex);
                    file.Close();
                    file.SetLength(fileLength);
                } finally
                {
                    file.Close();
                }
            }
        }
    }
}
