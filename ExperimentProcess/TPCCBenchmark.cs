using System;
using Orleans;
using Utilities;
using System.Linq;
using TPCC.Interfaces;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;

namespace ExperimentProcess
{
    public class TPCCBenchmark : IBenchmark
    {
        static double skewness = 0.01;
        static double hotRatio = 0.75;
        static WorkloadConfiguration config;

        public void generateBenchmark(WorkloadConfiguration workloadConfig)
        {
            config = workloadConfig;
            if (config.distribution == Distribution.UNIFORM) skewness = 0;
        }

        private Task<TransactionResult> Execute(IClusterClient client, int grainId, string functionName, FunctionInput input)
        {
            // todo: !!!! use tpcc interface
            var eventuallyConsistentGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(grainId);
            return eventuallyConsistentGrain.StartTransaction(functionName, input);
        }

        static Random C_rnd = new Random();
        static IDiscreteDistribution district_dist_uni = new DiscreteUniform(0, Constants.NUM_D_PER_W - 1, new Random());
        static IDiscreteDistribution ol_cnt_dist_uni = new DiscreteUniform(5, 15, new Random());
        static IDiscreteDistribution rbk_dist_uni = new DiscreteUniform(1, 100, new Random());
        static IDiscreteDistribution local_dist_uni = new DiscreteUniform(1, 100, new Random());
        static IDiscreteDistribution quantity_dist_uni = new DiscreteUniform(1, 10, new Random());

        static int error = 0;
        private static RequestData GenerateNewOrder()
        {
            var num_hot_wh = (int)(skewness * config.numWarehouse);
            var wh_dist_normal = new DiscreteUniform(num_hot_wh, config.numWarehouse - 1, new Random());
            IDiscreteDistribution wh_dist_hot = wh_dist_normal;
            if (num_hot_wh > 0) wh_dist_hot = new DiscreteUniform(0, num_hot_wh - 1, new Random());

            // generate W_ID (75% possibility to be hot warehouse)
            int W_ID;
            var rnd = new Random();
            if (rnd.Next(0, 100) < hotRatio * 100) W_ID = wh_dist_hot.Sample();
            else W_ID = wh_dist_normal.Sample();

            var grains = new List<int>();
            var D_ID = district_dist_uni.Sample();
            grains.Add(W_ID * Constants.NUM_D_PER_W + D_ID);
            var C_C_ID = C_rnd.Next(0, 1024);
            var C_ID = Helper.NURand(1023, 1, Constants.NUM_C_PER_D, C_C_ID) - 1;
            var ol_cnt = ol_cnt_dist_uni.Sample();
            var rbk = rbk_dist_uni.Sample();
            var itemsToBuy = new Dictionary<int, Tuple<int, int>>();  // <I_ID, <supply_warehouse, quantity>>
            var C_I_ID = C_rnd.Next(0, 8192);
            for (int i = 0; i < ol_cnt; i++)
            {
                int I_ID;
                if (i == ol_cnt - 1 && rbk == 1)
                {
                    I_ID = -1;   // generate 1% of error
                    error++;
                }
                else
                {
                    do I_ID = Helper.NURand(8191, 1, Constants.NUM_I, C_I_ID) - 1;
                    while (itemsToBuy.ContainsKey(I_ID));
                }
                var local = local_dist_uni.Sample() > 1;
                int supply_wh;
                if (local) supply_wh = W_ID;    // supply by home warehouse
                else                            // supply by remote warehouse
                {
                    do supply_wh = wh_dist_hot.Sample();   // select from a hot warehouse as remote supplier
                    while (supply_wh == W_ID);
                }
                var quantity = quantity_dist_uni.Sample();
                itemsToBuy.Add(I_ID, new Tuple<int, int>(supply_wh, quantity));

                var grainID = Helper.GetGrainID(supply_wh, I_ID, false);
                if (!grains.Contains(grainID)) grains.Add(grainID);
            }
            var req = new RequestData(W_ID, D_ID, C_ID, DateTime.Now, itemsToBuy);
            req.grains = grains;
            return req;
        }

        public Task<TransactionResult> newTransaction(IClusterClient client)
        {
            var data = GenerateNewOrder();
            var input = new FunctionInput(data.tpcc_input);
            var task = Execute(client, data.grains.First(), "NewOrder", input);
            return task;
        }
    }
}