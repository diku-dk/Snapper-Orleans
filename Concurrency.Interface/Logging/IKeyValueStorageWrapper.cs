using System.Threading.Tasks;

namespace Concurrency.Interface.Logging
{
    public interface IKeyValueStorageWrapper
    {
        Task<byte[]> Read(byte[] key);
        Task Write(byte[] key, byte[] value);
    }
}
