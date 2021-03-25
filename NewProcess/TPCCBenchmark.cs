using System;
using Orleans;
using Utilities;
using System.Linq;
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

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string functionName, FunctionInput input, Dictionary<int, Tuple<string, int>> grainAccessInfo)
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
            var grainAccessInfo = new Dictionary<int, Tuple<string, int>>();
            foreach (var grain in data.grains_in_namespace) grainAccessInfo.Add(grain.Item1, new Tuple<string, int>(grain.Item2, 1));
            var input = new FunctionInput(data.tpcc_input);
            var task = Execute(client, data.grains_in_namespace.First().Item1, "NewOrder", input, grainAccessInfo);
            return task;
        }
    }
}