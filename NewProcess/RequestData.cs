using System;
using TPCC.Interfaces;
using System.Collections.Generic;

namespace NewProcess
{
    [Serializable]
    public class RequestData
    {
        public List<int> grains;           // for SmallBank
        public NewOrderInput tpcc_input;

        public RequestData(List<int> grains)
        {
            this.grains = grains;
        }

        // for TPCC
        public RequestData(int C_ID, DateTime O_ENTRY_D, Dictionary<int, Tuple<int, int>> items)
        {
            tpcc_input = new NewOrderInput(C_ID, O_ENTRY_D, items);
        }

        // for big TPCC
        public RequestData(int D_ID, int C_ID, DateTime O_ENTRY_D, Dictionary<int, Tuple<int, int>> items)
        {
            tpcc_input = new NewOrderInput(D_ID, C_ID, O_ENTRY_D, items);
        }
    }
}
