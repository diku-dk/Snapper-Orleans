using System;
using Orleans;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;

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
            if (Constants.implementationType == ImplementationType.SNAPPER) grainNameSpace = "SmallBank.Grains.CustomerAccountGroupGrain";
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
            accountGrains.Sort();
            var grainIDList = new List<int>();
            var grainNameList = new List<string>();

            int firstGrainID = 0;
            Tuple<string, int> item1 = null;
            var item2 = transferAmountDistribution.Sample();
            var item3 = new List<Tuple<string, int>>();
            var first = true;
            foreach (var grainID in accountGrains)
            {
                if (first)
                {
                    first = false;
                    firstGrainID = grainID;
                    item1 = new Tuple<string, int>(grainID.ToString(), grainID);
                }
                else item3.Add(new Tuple<string, int>(grainID.ToString(), grainID));
                grainIDList.Add(grainID);
                grainNameList.Add(grainNameSpace);
            }
            // item1: from account
            // item2: the amount of money
            // item3: to accounts
            var args = new Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>(item1, item2, item3);
            var task = Execute(client, firstGrainID, "MultiTransfer", args, grainIDList, grainNameList);
            return task;
        }
    }
}