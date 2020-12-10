using System;
using Orleans;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;

namespace NewProcess
{
    public class SmallBankBenchmark : IBenchmark
    {
        bool isDet;
        WorkloadConfiguration config;
        IDiscreteDistribution transferAmountDistribution;
        
        public void generateBenchmark(WorkloadConfiguration workloadConfig, bool isDet)
        {
            this.isDet = isDet;
            config = workloadConfig;
            transferAmountDistribution = new DiscreteUniform(0, 10, new Random());
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

        private int getAccountForGrain(int grainId)
        {
            return grainId * config.numAccountsPerGroup;
        }

        public Task<TransactionResult> newTransaction(IClusterClient client, List<int> accountGrains)
        {
            // txn type must be MultiTransfer
            int groupId = 0;
            var grainAccessInfo = new Dictionary<int, int>();
            Tuple<string, int> item1 = null;
            float item2 = transferAmountDistribution.Sample();
            var item3 = new List<Tuple<string, int>>();
            bool first = true;
            foreach (var item in accountGrains)
            {
                if (first)
                {
                    first = false;
                    groupId = item;
                    grainAccessInfo.Add(groupId, 1);
                    int sourceId = getAccountForGrain(item);
                    item1 = new Tuple<string, int>(sourceId.ToString(), sourceId);
                    continue;
                }
                int destAccountId = getAccountForGrain(item);
                item3.Add(new Tuple<string, int>(destAccountId.ToString(), destAccountId));
                grainAccessInfo.Add(item, 1);
            }
            var args = new Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>(item1, item2, item3);
            var input = new FunctionInput(args);
            var task = Execute(client, groupId, "MultiTransfer", input, grainAccessInfo);
            return task;
        }
    }
}