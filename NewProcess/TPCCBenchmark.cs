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

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string functionName, FunctionInput input, Dictionary<Tuple<int, string>, int> grainAccessInfo)
        {
            switch (config.grainImplementationType)
            {
                case ImplementationType.SNAPPER:
                    switch (config.benchmark)
                    {
                        case BenchmarkType.TPCC:
                            var grain = client.GetGrain<ICustomerGrain>(grainId);
                            if (isDet) return grain.StartTransaction(grainAccessInfo, functionName, input);
                            else return grain.StartTransaction(functionName, input);
                        default:
                            throw new Exception($"Exception: Unknown benchmark {config.benchmark}");
                    }
                default:
                    throw new Exception("Exception: TPCC does not support orleans txn");
            }
        }

        public Task<TransactionResult> newTransaction(IClusterClient client, RequestData data)
        {
            var grainAccessInfo = new Dictionary<Tuple<int, string>, int>();   // <grainID, namespace, access time>
            foreach (var grain in data.grains_in_namespace) grainAccessInfo.Add(grain, 1);
            var input = new FunctionInput(data.tpcc_input);
            var task = Execute(client, data.firstGrainID, "NewOrder", input, grainAccessInfo);
            return task;
        }
    }
}