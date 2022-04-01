using System;
using Orleans;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;
using System.Linq;

namespace ExperimentProcess
{
    public class SmallBankBenchmark : IBenchmark
    {
        bool isDet;
        string grainNameSpace;
        IDiscreteDistribution transferAmountDistribution;

        public void GenerateBenchmark(WorkloadConfiguration _, bool isDet)
        {
            this.isDet = isDet;
            if (Constants.implementationType == ImplementationType.SNAPPER) grainNameSpace = "SmallBank.Grains.SnapperTransactionalAccountGrain";
            else grainNameSpace = "";
            transferAmountDistribution = new DiscreteUniform(0, 10, new Random());
        }

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string startFunc, object funcInput, List<int> grainIDList, List<string> grainNameList)
        {
            switch (Constants.implementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ISnapperTransactionalAccountGrain>(grainId);
                    if (isDet) return grain.StartTransaction(startFunc, funcInput, grainIDList, grainNameList);
                    else return grain.StartTransaction(startFunc, funcInput);
                case ImplementationType.ORLEANSEVENTUAL:
                    var eventuallyConsistentGrain = client.GetGrain<INonTransactionalAccountGrain>(grainId);
                    return eventuallyConsistentGrain.StartTransaction(startFunc, funcInput);
                case ImplementationType.ORLEANSTXN:
                    var txnGrain = client.GetGrain<IOrleansTransactionalAccountGrain>(grainId);
                    return txnGrain.StartTransaction(startFunc, funcInput);
                default:
                    return null;
            }
        }

        public Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data)
        {
            var accountGrains = data.grains;
            //accountGrains.Sort();

            var grainIDList = new List<int>(accountGrains);
            var grainNameList = Enumerable.Repeat(grainNameSpace, grainIDList.Count).ToList();

            var firstGrainID = accountGrains.First();
            accountGrains.RemoveAt(0);

            var money = transferAmountDistribution.Sample();
            var args = new Tuple<int, List<int>>(money, accountGrains);
            var task = Execute(client, firstGrainID, "MultiTransfer", args, grainIDList, grainNameList);
            return task;
        }
    }
}