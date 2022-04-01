using Newtonsoft.Json;
using MessagePack;
using Orleans.Transactions.Abstractions;

namespace OrleansSiloHost
{
    [MessagePackObject]
    public class KeyEntity
    {
        [Key(0)]
        public string ETag;

        public KeyEntity(string partitionKey)
        {
            ETag = partitionKey;
            CommittedSequenceId = 0;
            Metadata = JsonConvert.SerializeObject(new TransactionalStateMetaData());
        }

        [Key(1)]
        public long CommittedSequenceId { get; set; }

        [Key(2)]
        public string Metadata { get; set; }
    }
}