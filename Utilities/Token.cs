using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class BasicToken
    {
        public int lastEmitBid;
        public int lastEmitTid;
        public int lastCoordID;
        public int highestCommittedBid;
        public bool isLastEmitBidGlobal;
        // for local coordinator: <grainID, latest local bid emitted to this grain>
        // for global coordinator: <siloID, latest global bid emitted to this silo>
        public Dictionary<int, int> lastBidPerService;

        public BasicToken()
        {
            lastEmitBid = -1;
            lastEmitTid = -1;
            lastCoordID = -1;
            highestCommittedBid = -1;
            isLastEmitBidGlobal = false;
            lastBidPerService = new Dictionary<int, int>();
        }
    }

    [Serializable]
    public class LocalToken : BasicToken
    {
        // for global info
        public int lastEmitGlobalBid;

        public LocalToken() : base()
        {
            lastEmitGlobalBid = -1;
        }
    }
}