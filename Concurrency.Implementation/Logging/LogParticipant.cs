using System.Collections.Generic;
using MessagePack;

namespace Concurrency.Implementation.Logging
{
    [MessagePackObject]
    public class LogParticipant
    {
        [Key(0)]
        public int tid;
        [Key(1)]
        public int coordID;
        [Key(2)]
        public int sequenceNumber;
        [Key(3)]
        public HashSet<int> grains;

        public LogParticipant(int tid, int coordID, int sequenceNumber, HashSet<int> grains)
        {
            this.sequenceNumber = sequenceNumber;
            this.coordID = coordID;
            this.tid = tid;
            this.grains = grains;
        }
    }
}