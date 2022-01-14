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
        string grain_namespace;
        WorkloadConfiguration config;
        IDiscreteDistribution transferAmountDistribution;

        public void generateBenchmark(WorkloadConfiguration workloadConfig, bool isDet)
        {
            this.isDet = isDet;
            config = workloadConfig;
            if (Constants.implementationType == ImplementationType.SNAPPER) grain_namespace = "SmallBank.Grains.CustomerAccountGroupGrain";
            else grain_namespace = "";
            transferAmountDistribution = new DiscreteUniform(0, 10, new Random());
        }

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string startFunc, object funcInput, Dictionary<int, Tuple<string, int>> grainAccessInfo)
        {
            switch (Constants.implementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerAccountGroupGrain>(grainId);
                    if (isDet) return grain.StartTransaction(startFunc, funcInput, grainAccessInfo);
                    else return grain.StartTransaction(startFunc, funcInput);
                case ImplementationType.ORLEANSEVENTUAL:
                    var eventuallyConsistentGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(grainId);
                    return eventuallyConsistentGrain.StartTransaction(startFunc, funcInput);
                case ImplementationType.ORLEANSTXN:
                    var txnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(grainId);
                    return txnGrain.StartTransaction(startFunc, funcInput);
                default:
                    return null;
            }
        }

        public Task<TransactionResult> newTransaction(IClusterClient client, RequestData data)
        {
            var accountGrains = data.grains;
            accountGrains.Sort();
            var grainAccessInfo = new Dictionary<int, Tuple<string, int>>();
            /*
            grainAccessInfo.Add(accountGrains[0], new Tuple<string, int>(grain_namespace, 1));
            return Execute(client, accountGrains[0], "Balance", null, grainAccessInfo);*/

            // no deadlock
            /*
            accountGrains = new List<int>();
            for (int i = 0; i < 4; i++)
            {
                var id = index % 10000;
                accountGrains.Add(id);
                index++;
                if (index == 10000) index = 0;
                //Console.Write($"{id} ");
            }*/

            int groupId = 0;
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
                    grainAccessInfo.Add(item, new Tuple<string, int>(grain_namespace, 1));
                    item1 = new Tuple<string, int>(item.ToString(), item);
                    continue;
                }
                item3.Add(new Tuple<string, int>(item.ToString(), item));
                grainAccessInfo.Add(item, new Tuple<string, int>(grain_namespace, 1));
            }
            var args = new Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>(item1, item2, item3);
            var task = Execute(client, groupId, "MultiTransfer", args, grainAccessInfo);
            return task;
        }
    }
}