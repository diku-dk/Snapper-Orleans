using Newtonsoft.Json;
using Orleans.Transactions.Abstractions;

namespace OrleansSiloHost
{
    public class KeyEntity
    {
        public string ETag = "default";

        public KeyEntity()
        {
            CommittedSequenceId = 0;
            Metadata = JsonConvert.SerializeObject(new TransactionalStateMetaData());
        }

        public long CommittedSequenceId { get; set; }
        public string Metadata { get; set; }
    }
}