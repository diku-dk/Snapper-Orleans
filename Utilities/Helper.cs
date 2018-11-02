using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;

namespace Utilities
{
    public static class Helper
    {
        public static Guid convertUInt32ToGuid(UInt32 value)
        {
            var bytes = new byte[16];
            //Copying into the first four bytes
            BitConverter.GetBytes(value).CopyTo(bytes, 0);
            return new Guid(bytes);
        }

        public static byte[] serializeToByteArray<T>(T obj)
        {
            if(obj == null)
            {
                return null;
            }

            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            formatter.Serialize(stream, obj);
            return stream.ToArray();
        }

        public static T deserializeFromByteArray<T>(byte[] obj)
        {
            var formatter = new BinaryFormatter();
            var stream = new MemoryStream();
            //Clean up 
            stream.Write(obj, 0, obj.Length);
            stream.Seek(0, SeekOrigin.Begin);
            var result = (T) formatter.Deserialize(stream);
            return result;
        }
    }
}
