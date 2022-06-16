using System.Collections.Generic;
using MessagePack;

namespace Concurrency.Implementation.Logging
{
    [MessagePackObject]
    public class LogParticipant
    {
        [Key(0)]
        public long tid;
        [Key(1)]
        public int coordID;
        [Key(2)]
        public long sequenceNumber;
        [Key(3)]
        public HashSet<int> grains;

        public LogParticipant(long tid, int coordID, long sequenceNumber, HashSet<int> grains)
        {
            this.sequenceNumber = sequenceNumber;
            this.coordID = coordID;
            this.tid = tid;
            this.grains = grains;
        }
    }
}