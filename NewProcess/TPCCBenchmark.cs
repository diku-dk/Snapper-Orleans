using System;
using Orleans;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace NewProcess
{
    public class TPCCBenchmark : IBenchmark
    {
        bool isDet;
        WorkloadConfiguration config;

        public void generateBenchmark(WorkloadConfiguration workloadConfig, bool isDet)
        {
            this.isDet = isDet;
            config = workloadConfig;
        }

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string startFunc, object funcInput, Dictionary<int, Tuple<string, int>> grainAccessInfo)
        {
            switch (Constants.implementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerGrain>(grainId);
                    if (isDet) return grain.StartTransaction(startFunc, funcInput, grainAccessInfo);
                    else return grain.StartTransaction(startFunc, funcInput);
                case ImplementationType.ORLEANSEVENTUAL:
                    var egrain = client.GetGrain<IEventualCustomerGrain>(grainId);
                    return egrain.StartTransaction(startFunc, funcInput);
                default:
                    throw new Exception("Exception: TPCC does not support orleans txn");
            }
        }

        public Task<TransactionResult> newTransaction(IClusterClient client, RequestData data)
        {
            var grainAccessInfo = new Dictionary<int, Tuple<string, int>>();   // <grainID, namespace, access time>
            foreach (var grain in data.grains_in_namespace) grainAccessInfo.Add(grain.Key, new Tuple<string, int>(grain.Value, 1));
            var task = Execute(client, data.firstGrainID, "NewOrder", data.tpcc_input, grainAccessInfo);
            return task;
        }
    }
}