using System;
using TPCC.Grains;
using System.Collections.Generic;

namespace NewProcess
{
    [Serializable]
    public class RequestData
    {
        public List<int> grains;           // for SmallBank

        public NewOrderInput tpcc_input;
        public HashSet<Tuple<int, string>> grains_in_namespace;    // for TPCC

        public RequestData(List<int> grains)
        {
            this.grains = grains;
        }

        // for TPCC
        public RequestData(int C_ID, Dictionary<int, Tuple<int, int>> items)
        {
            tpcc_input = new NewOrderInput(C_ID, items);
        }
    }
}
