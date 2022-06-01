using MessagePack;

namespace Utilities
{
    public enum MsgType { WORKER_CONNECT, CONFIRM, WORKLOAD_INIT, WORKLOAD_INIT_ACK, RUN_EPOCH, RUN_EPOCH_ACK };

    [MessagePackObject]
    public class NetworkMessage
    {
        [Key(0)]
        public MsgType msgType;
        [Key(1)]
        public byte[] content;

        public NetworkMessage(MsgType msgType)
        {
            this.msgType = msgType;
        }

        public NetworkMessage(MsgType msgType, byte[] content)
        {
            this.msgType = msgType;
            this.content = content;
        }
    }
}