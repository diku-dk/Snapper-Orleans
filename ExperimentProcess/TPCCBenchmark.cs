using System;
using Orleans;
using Utilities;
using System.Linq;
using TPCC.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;

namespace ExperimentProcess
{
    public class TPCCBenchmark : IBenchmark
    {
        bool isDet;
        WorkloadConfiguration config;
        static IDiscreteDistribution wh_dist;
        static IDiscreteDistribution district_dist_uni = new DiscreteUniform(0, Constants.NUM_D_PER_W - 1, new Random());
        static IDiscreteDistribution ol_cnt_dist_uni = new DiscreteUniform(5, 15, new Random());
        static IDiscreteDistribution rbk_dist_uni = new DiscreteUniform(1, 100, new Random());
        static IDiscreteDistribution quantity_dist_uni = new DiscreteUniform(1, 10, new Random());
        static IDiscreteDistribution local_dist_uni = new DiscreteUniform(1, 100, new Random());

        public void generateBenchmark(WorkloadConfiguration workloadConfig, int tid)
        {
            config = workloadConfig;
            if (config.deterministicTxnPercent == 100) isDet = true;
            else if (config.deterministicTxnPercent == 0) isDet = false;
            else throw new Exception($"Exception: ExperimentProcess does not support hybrid workload");
            wh_dist = new DiscreteUniform(0, config.numWarehouse - 1, new Random());
        }

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string functionName, FunctionInput input, Dictionary<int, int> grainAccessInfo)
        {
            switch (config.grainImplementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<IWarehouseGrain>(grainId);
                    if (isDet) return grain.StartTransaction(grainAccessInfo, functionName, input);
                    else return grain.StartTransaction(functionName, input);
                case ImplementationType.ORLEANSEVENTUAL:
                    var eventuallyConsistentGrain = client.GetGrain<IOrleansEventuallyConsistentWarehouseGrain>(grainId);
                    return eventuallyConsistentGrain.StartTransaction(functionName, input);
                default:
                    throw new Exception("Exception: TPCC does not support orleans txn");
            }
        }

        private static RequestData GenerateNewOrder()
        {
            int W_ID = wh_dist.Sample();
            var grains = new List<int>();
            var D_ID = district_dist_uni.Sample();
            grains.Add(W_ID * Constants.NUM_D_PER_W + D_ID);
            var C_ID = Helper.NURand(1023, 1, Constants.NUM_C_PER_D, 0) - 1;
            var ol_cnt = ol_cnt_dist_uni.Sample();
            var rbk = rbk_dist_uni.Sample();
            var itemsToBuy = new Dictionary<int, Tuple<int, int>>();  // <I_ID, <supply_warehouse, quantity>>

            for (int i = 0; i < ol_cnt; i++)
            {
                int I_ID;
                if (i == ol_cnt - 1 && rbk == 1) I_ID = -1;
                else
                {
                    do I_ID = Helper.NURand(8191, 1, Constants.NUM_I, 0) - 1;
                    while (itemsToBuy.ContainsKey(I_ID));
                }

                var local = local_dist_uni.Sample() > 1;
                int supply_wh;
                if (local) supply_wh = W_ID;    // supply by home warehouse
                else                            // supply by remote warehouse
                {
                    do supply_wh = wh_dist.Sample();
                    while (supply_wh == W_ID);
                }
                var quantity = quantity_dist_uni.Sample();
                itemsToBuy.Add(I_ID, new Tuple<int, int>(supply_wh, quantity));

                if (I_ID != -1)
                {
                    var grainID = Helper.GetGrainID(supply_wh, I_ID, false);
                    if (!grains.Contains(grainID)) grains.Add(grainID);
                }
            }
            var req = new RequestData(W_ID, D_ID, C_ID, DateTime.Now, itemsToBuy);
            req.grains = grains;
            return req;
        }

        public Task<TransactionResult> newTransaction(IClusterClient client)
        {
            var data = GenerateNewOrder();
            var grainAccessInfo = new Dictionary<int, int>();
            foreach (var grain in data.grains) grainAccessInfo.Add(grain, 1);
            var input = new FunctionInput(data.tpcc_input);
            var task = Execute(client, data.grains.First(), "NewOrder", input, grainAccessInfo);
            return task;
        }
    }
}