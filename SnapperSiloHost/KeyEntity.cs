using Newtonsoft.Json;
using Orleans.Transactions.Abstractions;

namespace OrleansSnapperSiloHost
{
    public class KeyEntity
    {
        public string ETag;

        public KeyEntity(string partitionKey)
        {
            ETag = partitionKey;
            CommittedSequenceId = 0;
            Metadata = JsonConvert.SerializeObject(new TransactionalStateMetaData());
        }

        public long CommittedSequenceId { get; set; }
        public string Metadata { get; set; }
    }
}