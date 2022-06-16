using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class BasicToken
    {
        public long lastEmitBid;
        public long lastEmitTid;
        public int lastCoordID;
        public long highestCommittedBid;
        public bool isLastEmitBidGlobal;
        // for local coordinator: <grainID, latest local bid emitted to this grain>
        // for global coordinator: <siloID, latest global bid emitted to this silo>
        public Dictionary<int, long> lastBidPerService;

        // this info is only used for local coordinators
        public Dictionary<int, long> lastGlobalBidPerGrain;   // grainID, the global bid of the latest emitted local batch

        public BasicToken()
        {
            lastEmitBid = -1;
            lastEmitTid = -1;
            lastCoordID = -1;
            highestCommittedBid = -1;
            isLastEmitBidGlobal = false;
            lastBidPerService = new Dictionary<int, long>();
            lastGlobalBidPerGrain = new Dictionary<int, long>();
        }
    }

    [Serializable]
    public class LocalToken : BasicToken
    {
        // for global info
        public long lastEmitGlobalBid;

        public LocalToken() : base()
        {
            lastEmitGlobalBid = -1;
        }
    }
}