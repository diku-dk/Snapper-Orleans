using System;
using TPCC.Grains;
using System.Collections.Generic;

namespace SnapperExperimentProcess
{
    [Serializable]
    public class RequestData
    {
        // for SmallBank
        public bool isDistTxn;
        public List<int> grains;

        // for TPCC
        public int firstGrainID;
        public NewOrderInput tpcc_input;
        public Dictionary<int, string> grains_in_namespace;

        public RequestData(bool isDistTxn, List<int> grains)
        {
            this.isDistTxn = isDistTxn;
            this.grains = grains;
        }

        // for TPCC
        public RequestData(int firstGrainID, int C_ID, Dictionary<int, Tuple<int, int>> items)
        {
            this.firstGrainID = firstGrainID;
            tpcc_input = new NewOrderInput(C_ID, items);
        }
    }
}
