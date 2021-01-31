using System;

namespace Utilities
{
    public enum MsgType { WORKER_CONNECT, WORKLOAD_INIT, WORKLOAD_INIT_ACK, RUN_EPOCH, RUN_EPOCH_ACK };

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
