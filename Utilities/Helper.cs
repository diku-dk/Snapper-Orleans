using System;
using System.Collections.Generic;
using System.Text;

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
    }
}
