using MathNet.Numerics.Distributions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Utilities;

namespace SnapperExperimentProcess
{
    using SharedRequest = Dictionary<int, Queue<Tuple<bool, RequestData>>>;

    public class WorkloadGenerator
    {
        readonly int BASE_NUM_NEWORDER;
        readonly SiloConfiguration siloConfig;
        readonly Workload workload;
        static IDiscreteDistribution detDistribution = new DiscreteUniform(0, 99, new Random());

        public WorkloadGenerator(SiloConfiguration siloConfig, Workload workload)
        {
            this.siloConfig = siloConfig;
            this.workload = workload;
            BASE_NUM_NEWORDER = 20000 * siloConfig.numCPUPerSilo / Constants.numCPUBasic;
        }

        public void GenerateSmallBankWorkload(SharedRequest shared_requests)
        {
            var numGrainPerSilo = Helper.GetNumGrainPerSilo(siloConfig.numCPUPerSilo);
            switch (workload.distribution)
            {
                case Distribution.NOCONFLICT:
                    // this is only used for comparing ACT and OrleansTxn
                    Console.WriteLine($"Generate NOCONFLICT data for SmallBank, txnSize = {workload.txnSize}, numWriter = {workload.numWriter}");
                    var rnd = new Random();
                    var grains = new int[] { 0, 1, 2, 3 };
                    for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
                    {
                        for (int txn = 0; txn < 50000; txn++)
                        {
                            var grainsPerTxn = grains.OrderBy(x => rnd.Next()).ToList();
                            shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(grainsPerTxn)));
                        }
                    }
                    break;
                case Distribution.UNIFORM:
                    Console.WriteLine($"Generate UNIFORM data for SmallBank, txnSize = {workload.txnSize}");
                    {
                        var grainDist = new DiscreteUniform(0, numGrainPerSilo - 1, new Random());  // [0, numGrainPerSilo - 1]
                        for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
                        {
                            for (int txn = 0; txn < 500000; txn++)
                            {
                                var grainsPerTxn = new List<int>();
                                for (int k = 0; k < workload.txnSize; k++)
                                {
                                    var grainID = grainDist.Sample();
                                    while (grainsPerTxn.Contains(grainID)) grainID = grainDist.Sample();
                                    grainsPerTxn.Add(grainID);
                                }
                                Debug.Assert(grainsPerTxn.Count == workload.txnSize);
                                if (workload.noDeadlock) grainsPerTxn.Sort();
                                shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(grainsPerTxn)));
                            }
                        }
                    }
                    break;
                case Distribution.HOTSPOT:
                    var numTxnPerEpoch = 25000 * siloConfig.numCPUPerSilo / Constants.numCPUBasic;
                    if (siloConfig.benchmarkType == BenchmarkType.TPCC) numTxnPerEpoch = 10000 * siloConfig.numCPUPerSilo / Constants.numCPUBasic;
                    int numHotGrain = (int)(Constants.grainSkewness * numGrainPerSilo);
                    var numHotGrainPerTxn = Constants.txnSkewness * workload.txnSize;
                    Console.WriteLine($"Generate data for HOTRECORD, {numHotGrain} hot grains, {numHotGrainPerTxn} hot grain per txn...");
                    var normal_dist = new DiscreteUniform(numHotGrain, numGrainPerSilo - 1, new Random());
                    DiscreteUniform hot_dist = null;
                    if (numHotGrain > 0) hot_dist = new DiscreteUniform(0, numHotGrain - 1, new Random());
                    for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
                    {
                        for (int txn = 0; txn < numTxnPerEpoch; txn++)
                        {
                            var grainsPerTxn = new List<int>();
                            for (int normal = 0; normal < workload.txnSize - numHotGrainPerTxn; normal++)
                            {
                                var normalGrain = normal_dist.Sample();
                                while (grainsPerTxn.Contains(normalGrain)) normalGrain = normal_dist.Sample();
                                grainsPerTxn.Add(normalGrain);
                            }
                            for (int hot = 0; hot < numHotGrainPerTxn; hot++)
                            {
                                var hotGrain = hot_dist.Sample();
                                while (grainsPerTxn.Contains(hotGrain)) hotGrain = hot_dist.Sample();
                                grainsPerTxn.Add(hotGrain);
                            }
                            shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(grainsPerTxn)));
                        }
                    }
                    break;
                case Distribution.ZIPFIAN:    // read data from file
                    Debug.Assert(workload.txnSize == 4);
                    var zipf = workload.zipfianConstant;
                    Console.WriteLine($"Read data from files, txnsize = {workload.txnSize}, zipf = {zipf}");
                    var prefix = Constants.dataPath + $@"zipfian_workload\zipf{zipf}_";

                    // read data from files
                    for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
                    {
                        string line;
                        var path = prefix + $@"epoch{epoch}.txt";
                        var file = new StreamReader(path);
                        while ((line = file.ReadLine()) != null)
                        {
                            var grainsPerTxn = new List<int>();
                            var IDs = line.Split(' ');
                            for (int i = 0; i < workload.txnSize; i++) grainsPerTxn.Add(int.Parse(IDs[i]));
                            if (workload.noDeadlock) grainsPerTxn.Sort();
                            shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(grainsPerTxn)));
                        }
                        file.Close();
                    }
                    break;
                default:
                    throw new Exception("Exception: Unknown distribution. ");
            }
        }

        public void GenerateTPCCWorkload(SharedRequest shared_requests)
        {
            Debug.Assert(workload.distribution != Distribution.ZIPFIAN);
            Console.WriteLine($"Generate {workload.distribution} data for TPCC. ");
            for (int epoch = 0; epoch < Constants.numEpoch; epoch++) GenerateNewOrder(epoch, shared_requests);
        }

        void GenerateNewOrder(int epoch, SharedRequest shared_requests)
        {
            var numRound = siloConfig.numCPUPerSilo / Constants.numCPUBasic;
            if (siloConfig.implementationType == ImplementationType.NONTXN) numRound *= 3;

            var remote_count = 0;
            var txn_size = new List<int>();
            var tpccManager = new TPCCManager();
            tpccManager.Init(siloConfig.numCPUPerSilo, siloConfig.NUM_OrderGrain_PER_D);
            var NUM_W_PER_SILO = Helper.GetNumWarehousePerSilo(siloConfig.numCPUPerSilo);
            Console.WriteLine($"Generate TPCC workload for epoch {epoch}, numRound = {numRound}");
            for (int round = 0; round < numRound; round++)
            {
                DiscreteUniform hot = null;
                DiscreteUniform wh_dist = null;
                DiscreteUniform hot_wh_dist = null;
                DiscreteUniform district_dist = null;
                DiscreteUniform hot_district_dist = null;
                var all_wh_dist = new DiscreteUniform(0, NUM_W_PER_SILO - 1, new Random());
                if (workload.distribution == Distribution.HOTSPOT)
                {
                    // hot set
                    var num_hot_wh = (int)(0.5 * NUM_W_PER_SILO);
                    var num_hot_district = (int)(0.1 * Constants.NUM_D_PER_W);
                    hot_wh_dist = new DiscreteUniform(0, num_hot_wh - 1, new Random());
                    wh_dist = new DiscreteUniform(num_hot_wh, NUM_W_PER_SILO - 1, new Random());
                    hot_district_dist = new DiscreteUniform(0, num_hot_district - 1, new Random());
                    district_dist = new DiscreteUniform(num_hot_district, Constants.NUM_D_PER_W - 1, new Random());
                    hot = new DiscreteUniform(0, 99, new Random());
                }
                else
                {
                    Debug.Assert(workload.distribution == Distribution.UNIFORM);
                    wh_dist = new DiscreteUniform(0, NUM_W_PER_SILO - 1, new Random());
                    district_dist = new DiscreteUniform(0, Constants.NUM_D_PER_W - 1, new Random());
                }
                var ol_cnt_dist_uni = new DiscreteUniform(5, 15, new Random());
                var rbk_dist_uni = new DiscreteUniform(1, 100, new Random());
                var local_dist_uni = new DiscreteUniform(1, 100, new Random());
                var quantity_dist_uni = new DiscreteUniform(1, 10, new Random());

                for (int txn = 0; txn < BASE_NUM_NEWORDER; txn++)
                {
                    int W_ID;
                    int D_ID;
                    if (workload.distribution == Distribution.HOTSPOT)
                    {
                        var is_hot = false;
                        var p = hot.Sample();
                        if (p < 75)    // 75% choose from hot set
                        {
                            is_hot = true;
                            W_ID = hot_wh_dist.Sample();
                            D_ID = hot_district_dist.Sample();
                        }
                        else
                        {
                            W_ID = wh_dist.Sample();
                            D_ID = district_dist.Sample();
                        }
                    }
                    else
                    {
                        W_ID = wh_dist.Sample();
                        D_ID = district_dist.Sample();
                    }
                    var C_ID = Helper.NURand(1023, 1, Constants.NUM_C_PER_D, 0) - 1;
                    var firstGrainID = tpccManager.GetCustomerGrain(W_ID, D_ID);
                    var grains = new Dictionary<int, string>();
                    grains.Add(tpccManager.GetItemGrain(W_ID), "TPCC.Grains.ItemGrain");
                    grains.Add(tpccManager.GetWarehouseGrain(W_ID), "TPCC.Grains.WarehouseGrain");
                    grains.Add(firstGrainID, "TPCC.Grains.CustomerGrain");
                    grains.Add(tpccManager.GetDistrictGrain(W_ID, D_ID), "TPCC.Grains.DistrictGrain");
                    grains.Add(tpccManager.GetOrderGrain(W_ID, D_ID, C_ID), "TPCC.Grains.OrderGrain");
                    var ol_cnt = ol_cnt_dist_uni.Sample();
                    var rbk = rbk_dist_uni.Sample();
                    //rbk = 0;
                    var itemsToBuy = new Dictionary<int, Tuple<int, int>>();  // <I_ID, <supply_warehouse, quantity>>

                    var remote_flag = false;

                    for (int i = 0; i < ol_cnt; i++)
                    {
                        int I_ID;

                        if (i == ol_cnt - 1 && rbk == 1) I_ID = -1;
                        else
                        {
                            do I_ID = Helper.NURand(8191, 1, Constants.NUM_I, 0) - 1;
                            while (itemsToBuy.ContainsKey(I_ID));
                        }

                        int supply_wh;
                        var local = local_dist_uni.Sample() > 1;
                        if (NUM_W_PER_SILO == 1 || local) supply_wh = W_ID;    // supply by home warehouse
                        else   // supply by remote warehouse
                        {
                            remote_flag = true;
                            do supply_wh = all_wh_dist.Sample();
                            while (supply_wh == W_ID);
                        }
                        var quantity = quantity_dist_uni.Sample();
                        itemsToBuy.Add(I_ID, new Tuple<int, int>(supply_wh, quantity));

                        if (I_ID != -1)
                        {
                            var grainID = tpccManager.GetStockGrain(supply_wh, I_ID);
                            if (!grains.ContainsKey(grainID)) grains.Add(grainID, "TPCC.Grains.StockGrain");
                        }
                    }
                    if (remote_flag) remote_count++;
                    txn_size.Add(grains.Count);
                    var req = new RequestData(firstGrainID, C_ID, itemsToBuy);
                    req.grains_in_namespace = grains;
                    shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), req));
                }
            }
            var numTxn = BASE_NUM_NEWORDER * numRound;
            Console.WriteLine($"siloCPU = {siloConfig.numCPUPerSilo}, epoch = {epoch}, remote wh rate = {remote_count * 100.0 / numTxn}%, txn_size_ave = {txn_size.Average()}");
        }

        bool isDet()
        {
            if (workload.pactPercent == 0) return false;
            else if (workload.pactPercent == 100) return true;

            var sample = detDistribution.Sample();
            if (sample < workload.pactPercent) return true;
            else return false;
        }
    }
}
