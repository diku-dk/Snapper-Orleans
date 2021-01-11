using System;
using TPCC.Interfaces;
using System.Collections.Generic;

namespace ExperimentProcess
{
    [Serializable]
    public class RequestData
    {
        public List<int> grains;    // for SmallBank

        // for TPCC
        public int W_ID;
        public int D_ID;
        public NewOrderInput tpcc_input;

        public RequestData(List<int> grains)
        {
            this.grains = grains;
        }

        public RequestData(int W_ID, int D_ID, int C_ID, DateTime O_ENTRY_D, Dictionary<int, Tuple<int, int>> items)
        {
            this.W_ID = W_ID;
            this.D_ID = D_ID;
            tpcc_input = new NewOrderInput(C_ID, O_ENTRY_D, items);
        }
    }
}
