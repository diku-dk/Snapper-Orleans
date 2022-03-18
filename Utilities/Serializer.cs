using System.IO;
using MsgPack.Serialization;

namespace Utilities
{
    public interface ISerializer
    {
        byte[] serialize<T>(T obj);
        T deserialize<T>(byte[] obj);
    }

    public class MsgPackSerializer : ISerializer
    {
        public byte[] serialize<T>(T obj)
        {
            //var start = DateTime.Now;
            if (obj == null) return null;
            var stream = new MemoryStream();
            var serializer = MessagePackSerializer.Get<T>();
            serializer.Pack(stream, obj);
            //Console.WriteLine($"MsgPack takes {(DateTime.Now - start).TotalMilliseconds}ms. ");
            return stream.ToArray();
        }

        public T deserialize<T>(byte[] obj)
        {
            var stream = new MemoryStream();
            //Clean up 
            stream.Write(obj, 0, obj.Length);
            stream.Seek(0, SeekOrigin.Begin);
            var serializer = MessagePackSerializer.Get<T>();
            var result = serializer.Unpack(stream);
            return result;
        }
    }
}
