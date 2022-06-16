using System;
using System.Collections.Generic;

namespace Concurrency.Implementation.Logging
{
    [Serializable]
    public class LogParticipant
    {
        public int tid;
        public int coordID;
        public int sequenceNumber;
        public HashSet<int> grains;

        public LogParticipant(int sequenceNumber, int coordID, int tid, HashSet<int> grains)
        {
            this.sequenceNumber = sequenceNumber;
            this.coordID = coordID;
            this.tid = tid;
            this.grains = grains;
        }
    }
}