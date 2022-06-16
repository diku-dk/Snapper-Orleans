using System;
using Orleans;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Transactions;

namespace SnapperExperimentProcess
{
    public class TPCCBenchmark : IBenchmark
    {
        ImplementationType implementationType;
        bool isDet;

        public void GenerateBenchmark(ImplementationType implementationType, bool isDet, bool noDeadlock = false)
        {
            this.implementationType = implementationType;
            this.isDet = isDet;
        }

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string startFunc, object funcInput, Dictionary<int, Tuple<string, int>> grainAccessInfo)
        {
            switch (implementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerGrain>(grainId);
                    if (isDet) return grain.StartTransaction(startFunc, funcInput, grainAccessInfo);
                    else return grain.StartTransaction(startFunc, funcInput);
                case ImplementationType.NONTXN:
                    var egrain = client.GetGrain<IEventualCustomerGrain>(grainId);
                    return egrain.StartTransaction(startFunc, funcInput);
                default:
                    throw new Exception("Exception: TPCC does not support orleans txn");
            }
        }

        public Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data)
        {
            var grainAccessInfo = new Dictionary<int, Tuple<string, int>>();   // <grainID, namespace, access time>
            foreach (var grain in data.grains_in_namespace) grainAccessInfo.Add(grain.Key, new Tuple<string, int>(grain.Value, 1));
            var task = Execute(client, data.firstGrainID, "NewOrder", data.tpcc_input, grainAccessInfo);
            return task;
        }

        public Task<TransactionResult> NewTransactionWithNOOP(IClusterClient client, RequestData data, int numWriter)
        {
            throw new NotImplementedException();
        }
    }
}