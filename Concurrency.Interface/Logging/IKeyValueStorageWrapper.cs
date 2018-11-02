using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Interface.Logging
{
    public interface IKeyValueStorageWrapper
    {
        Task<byte[]> Read(Guid key);
        Task Write(Guid key, byte[] value);
    }
}
