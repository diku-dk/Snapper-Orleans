using System;
using Orleans;
using Utilities;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;
using SmallBank.Interfaces;

namespace ExperimentProcess
{
    public class SmallBankBenchmark : IBenchmark
    {
        WorkloadConfiguration config;
        IDiscreteDistribution hot_dist;
        IDiscreteDistribution normal_dist;
        IDiscreteDistribution transferAmountDistribution;

        public void generateBenchmark(WorkloadConfiguration workloadConfig, int tid)
        {
            config = workloadConfig;
            transferAmountDistribution = new DiscreteUniform(0, 10, new Random());
            var numGrain = config.numAccounts / config.numAccountsPerGroup;
            switch (config.distribution)
            {
                case Distribution.UNIFORM:
                    normal_dist = new DiscreteUniform(0, numGrain - 1, new Random());
                    break;
                case Distribution.HOTRECORD:
                    var num_hot_grain = (int)(Constants.skewness * numGrain);
                    hot_dist = new DiscreteUniform(0, num_hot_grain - 1, new Random());
                    normal_dist = new DiscreteUniform(num_hot_grain, numGrain - 1, new Random());
                    break;
                default:
                    throw new Exception("Exception: Unknown distribution. ");
            }
        }

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string functionName, FunctionInput input, Dictionary<int, int> grainAccessInfo)
        {
            if (config.grainImplementationType == ImplementationType.SNAPPER)
            {
                var grain = client.GetGrain<ICustomerAccountGroupGrain>(grainId);
                return grain.StartTransaction(functionName, input);
            }
            else if (config.grainImplementationType == ImplementationType.ORLEANSEVENTUAL)
            {
                var eventuallyConsistentGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(grainId);
                return eventuallyConsistentGrain.StartTransaction(functionName, input);
            }
            else throw new Exception("Exception: SmallBank does not support orleans txn");
        }

        private int getAccountForGrain(int grainId)
        {
            return grainId * config.numAccountsPerGroup;
        }

        public Task<TransactionResult> newTransaction(IClusterClient client)
        {
            if (config.mixture.Sum() > 0) throw new Exception("Exception: ExperimentProcess only support MultiTransfer for SmallBankBenchmark");
            var numGrainPerTxn = config.numGrainsMultiTransfer;
            var numHotGrainPerTxn = Constants.hotRatio * numGrainPerTxn;
            if (config.distribution == Distribution.UNIFORM) numHotGrainPerTxn = 0;
            var accountGrains = new List<int>();
            for (int normal = 0; normal < numGrainPerTxn - numHotGrainPerTxn; normal++)
            {
                var normalGrain = normal_dist.Sample();
                while (accountGrains.Contains(normalGrain)) normalGrain = normal_dist.Sample();
                accountGrains.Add(normalGrain);
            }
            for (int hot = 0; hot < numHotGrainPerTxn; hot++)
            {
                var hotGrain = hot_dist.Sample();
                while (accountGrains.Contains(hotGrain)) hotGrain = hot_dist.Sample();
                accountGrains.Add(hotGrain);
            }

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