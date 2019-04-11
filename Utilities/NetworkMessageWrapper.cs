using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    public enum MsgType { WORKLOAD_CONFIG, WORKLOAD_RESULTS };

    [Serializable]
    public class NetworkMessageWrapper 
    {
        public MsgType msgType;
        public byte[] contents;

        public NetworkMessageWrapper(MsgType msgType)
        {
            this.msgType = msgType;
        }
    }
}
