using Orleans;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

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

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string functionName, FunctionInput input, Dictionary<int, int> grainAccessInfo)
        {
            switch (config.grainImplementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerAccountGroupGrain>(grainId);
                    if (isDet) return grain.StartTransaction(grainAccessInfo, functionName, input);
                    else return grain.StartTransaction(functionName, input);
                case ImplementationType.ORLEANSEVENTUAL:
                    var eventuallyConsistentGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(grainId);
                    return eventuallyConsistentGrain.StartTransaction(functionName, input);
                case ImplementationType.ORLEANSTXN:
                    var txnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(grainId);
                    return txnGrain.StartTransaction(functionName, input);
                default:
                    return null;
            }
        }

        public Task<TransactionResult> newTransaction(IClusterClient client, RequestData data)
        {
            var grainAccessInfo = new Dictionary<int, int>();
            foreach (var grain in data.grains) grainAccessInfo.Add(grain, 1);
            var input = new FunctionInput(data.tpcc_input);
            var task = Execute(client, data.grains.First(), "NewOrder", input, grainAccessInfo);
            return task;
        }
    }
}