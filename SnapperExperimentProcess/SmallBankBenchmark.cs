using System;
using Orleans;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;
using System.Diagnostics;
using Orleans.Transactions;

namespace SnapperExperimentProcess
{
    public class SmallBankBenchmark : IBenchmark
    {
        bool isDet;
        bool noDeadlock;
        string grain_namespace;
        ImplementationType implementationType;
        IDiscreteDistribution transferAmountDistribution;

        public void GenerateBenchmark(ImplementationType implementationType, bool isDet, bool noDeadlock = false)
        {
            this.isDet = isDet;
            this.noDeadlock = noDeadlock;
            this.implementationType = implementationType;
            if (implementationType == ImplementationType.SNAPPER) grain_namespace = "SmallBank.Grains.CustomerAccountGroupGrain";
            else grain_namespace = "";
            transferAmountDistribution = new DiscreteUniform(0, 10, new Random());
        }

        Task<TransactionResult> Execute(IClusterClient client, int grainId, string startFunc, object funcInput, Dictionary<int, Tuple<string, int>> grainAccessInfo)
        {
            switch (implementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerAccountGroupGrain>(grainId);
                    if (isDet) return grain.StartTransaction(startFunc, funcInput, grainAccessInfo);
                    else return grain.StartTransaction(startFunc, funcInput);
                case ImplementationType.NONTXN:
                    var eventuallyConsistentGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(grainId);
                    return eventuallyConsistentGrain.StartTransaction(startFunc, funcInput);
                case ImplementationType.ORLEANSTXN:
                    var txnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(grainId);
                    return txnGrain.StartTransaction(startFunc, funcInput);
                default:
                    return null;
            }
        }

        public Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data)
        {
            var accountGrains = data.grains;
            var grainAccessInfo = new Dictionary<int, Tuple<string, int>>();

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
            var startFunc = "MultiTransfer";
            if (noDeadlock) startFunc = "MultiTransferNoDeadlock";
            var task = Execute(client, groupId, startFunc, args, grainAccessInfo);
            return task;
        }

        public Task<TransactionResult> NewTransactionWithNOOP(IClusterClient client, RequestData data, int numWriter)
        {
            var accountGrains = data.grains;
            var grainAccessInfo = new Dictionary<int, Tuple<string, int>>();

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
            var args = new Tuple<Tuple<string, int>, float, List<Tuple<string, int>>, int>(item1, item2, item3, numWriter);
            var task = ExecuteWithNOOP(client, groupId, "MultiTransferWithNOOP", args, grainAccessInfo);
            return task;
        }

        Task<TransactionResult> ExecuteWithNOOP(IClusterClient client, int grainId, string startFunc, object funcInput, Dictionary<int, Tuple<string, int>> grainAccessInfo)
        {
            switch (implementationType)
            {
                case ImplementationType.SNAPPER:
                    Debug.Assert(isDet == false);
                    var grain = client.GetGrain<ICustomerAccountGroupGrain>(grainId);
                    return grain.StartTransactionAndGetTime(startFunc, funcInput);
                case ImplementationType.ORLEANSTXN:
                    var txnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(grainId);
                    return txnGrain.StartTransaction(startFunc, funcInput);
                default:
                    throw new Exception($"Exception: unsupported implementation type {implementationType} for ExecuteWithNOOP.");
            }
        }
    }
}