using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;

namespace Concurrency.Implementation.Logging
{
    class InMemoryStorageWrapper : IKeyValueStorageWrapper
    {
        private List<Tuple<byte[], byte[]>> log;

        public InMemoryStorageWrapper()
        {
            log = new List<Tuple<byte[], byte[]>>();
        }

        public Task<byte[]> Read(byte[] key)
        {
            throw new NotImplementedException();
        }

        public Task Write(byte[] key, byte[] value)
        {
            log.Add(new Tuple<byte[], byte[]>(key, value));
            return Task.CompletedTask;
        }
    }
}
