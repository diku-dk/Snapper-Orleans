using System;
using Orleans;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace SnapperExperimentProcess
{
    public class TPCCBenchmark : IBenchmark
    {
        bool isDet;

        public void GenerateBenchmark(WorkloadConfiguration _, bool isDet)
        {
            this.isDet = isDet;
        }

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string startFunc, object funcInput, List<int> grainIDList, List<string> grainNameList)
        {
            switch (Constants.implementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerGrain>(grainId);
                    if (isDet) return grain.StartTransaction(startFunc, funcInput, grainIDList, grainNameList);
                    else return grain.StartTransaction(startFunc, funcInput);
                case ImplementationType.ORLEANSEVENTUAL:
                    var egrain = client.GetGrain<INTCustomerGrain>(grainId);
                    return egrain.StartTransaction(startFunc, funcInput);
                default:
                    throw new Exception("Exception: TPCC does not support orleans txn");
            }
        }

        public Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data)
        {
            var grainIDList = new List<int>();
            var grainNameList = new List<string>();
            foreach (var grain in data.grains_in_namespace)
            {
                grainIDList.Add(grain.Key);
                grainNameList.Add(grain.Value);
            }
            var task = Execute(client, data.firstGrainID, "NewOrder", data.tpcc_input, grainIDList, grainNameList);
            return task;
        }
    }
}