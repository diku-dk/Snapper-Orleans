using Utilities;
using System;
using System.Collections.Generic;
using System.Linq;
using MathNet.Numerics.Distributions;
using System.Diagnostics;
using System.IO;

namespace SnapperExperimentProcess
{
    public class WorkloadGenerator
    {
        readonly int workerID;
        readonly WorkloadConfiguration workload;
        readonly IDiscreteDistribution numSiloDist = new DiscreteUniform(0, 99, new Random());
        readonly IDiscreteDistribution detDistribution = new DiscreteUniform(0, 99, new Random());
        Dictionary<int, Queue<Tuple<bool, RequestData>>> shared_requests;  // <epoch, <isDet, grainIDs>>

        public WorkloadGenerator(
            int workerID,
            WorkloadConfiguration workload,
            Dictionary<int, Queue<Tuple<bool, RequestData>>> shared_requests)
        {
            this.workerID = workerID;
            this.workload = workload;
            this.shared_requests = shared_requests;
        }

        public void GenerateWorkload()
        {
            if (Constants.benchmark == BenchmarkType.SMALLBANK) InitializeSmallBankWorkload();
            else if (Constants.benchmark == BenchmarkType.TPCC) InitializeTPCCWorkload();
        }

        private bool isDet()
        {
            if (workload.pactPercent == 0) return false;
            else if (workload.pactPercent == 100) return true;

            var sample = detDistribution.Sample();
            if (sample < workload.pactPercent) return true;
            else return false;
        }

        public void GenerateNewOrder(int epoch)
        {
            var numRound = Constants.numCPUPerSilo / Constants.numCPUBasic;
            if (Constants.implementationType == ImplementationType.ORLEANSEVENTUAL) numRound *= 3;

            var remote_count = 0;
            var txn_size = new List<int>();
            Console.WriteLine($"Generate TPCC workload for epoch {epoch}, numRound = {numRound}");
            for (int round = 0; round < numRound; round++)
            {
                DiscreteUniform hot = null;
                DiscreteUniform wh_dist = null;
                DiscreteUniform hot_wh_dist = null;
                DiscreteUniform district_dist = null;
                DiscreteUniform hot_district_dist = null;
                var all_wh_dist = new DiscreteUniform(0, Constants.NUM_W_PER_SILO - 1, new Random());
                if (Constants.distribution == Distribution.HOTRECORD)
                {
                    // hot set
                    var num_hot_wh = (int)(0.5 * Constants.NUM_W_PER_SILO);
                    var num_hot_district = (int)(0.1 * Constants.NUM_D_PER_W);
                    hot_wh_dist = new DiscreteUniform(0, num_hot_wh - 1, new Random());
                    wh_dist = new DiscreteUniform(num_hot_wh, Constants.NUM_W_PER_SILO - 1, new Random());
                    hot_district_dist = new DiscreteUniform(0, num_hot_district - 1, new Random());
                    district_dist = new DiscreteUniform(num_hot_district, Constants.NUM_D_PER_W - 1, new Random());
                    hot = new DiscreteUniform(0, 99, new Random());
                }
                else
                {
                    Debug.Assert(Constants.distribution == Distribution.UNIFORM);
                    wh_dist = new DiscreteUniform(0, Constants.NUM_W_PER_SILO - 1, new Random());
                    district_dist = new DiscreteUniform(0, Constants.NUM_D_PER_W - 1, new Random());
                }
                var ol_cnt_dist_uni = new DiscreteUniform(5, 15, new Random());
                var rbk_dist_uni = new DiscreteUniform(1, 100, new Random());
                var local_dist_uni = new DiscreteUniform(1, 100, new Random());
                var quantity_dist_uni = new DiscreteUniform(1, 10, new Random());

                for (int txn = 0; txn < Constants.BASE_NUM_NEWORDER; txn++)
                {
                    int W_ID;
                    int D_ID;
                    if (Constants.distribution == Distribution.HOTRECORD)
                    {
                        var p = hot.Sample();
                        if (p < 75)    // 75% choose from hot set
                        {
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
                    var firstGrainID = Helper.GetCustomerGrain(W_ID, D_ID);
                    var grains = new Dictionary<int, string>();
                    grains.Add(Helper.GetItemGrain(W_ID), "TPCC.Grains.ItemGrain");
                    grains.Add(Helper.GetWarehouseGrain(W_ID), "TPCC.Grains.WarehouseGrain");
                    grains.Add(firstGrainID, "TPCC.Grains.CustomerGrain");
                    grains.Add(Helper.GetDistrictGrain(W_ID, D_ID), "TPCC.Grains.DistrictGrain");
                    grains.Add(Helper.GetOrderGrain(W_ID, D_ID, C_ID), "TPCC.Grains.OrderGrain");
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
                        if (Constants.NUM_W_PER_SILO == 1 || local) supply_wh = W_ID;    // supply by home warehouse
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
                            var grainID = Helper.GetStockGrain(supply_wh, I_ID);
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
            var numTxn = Constants.BASE_NUM_NEWORDER * numRound;
            Console.WriteLine($"siloCPU = {Constants.numCPUPerSilo}, epoch = {epoch}, remote wh rate = {remote_count * 100.0 / numTxn}%, txn_size_ave = {txn_size.Average()}");
        }

        private void InitializeTPCCWorkload()
        {
            Debug.Assert(Constants.distribution != Distribution.ZIPFIAN);
            Console.WriteLine($"Generate {Constants.distribution} data for TPCC. ");
            for (int epoch = 0; epoch < Constants.numEpoch; epoch++) GenerateNewOrder(epoch);
        }

        private  int SelectNumSilo(int txnSize)
        {
            if (Constants.multiSilo == false) return 1;
            
            var sample = numSiloDist.Sample(); // sample = [0, 100)
            if (sample < workload.distPercent) return 2;
            else return 1;
        }

        private  void InitializeSmallBankWorkload()
        {
            var numTxnPerEpoch = Constants.BASE_NUM_MULTITRANSFER * 10 * Constants.numCPUPerSilo / Constants.numCPUBasic;
            if (Constants.implementationType == ImplementationType.ORLEANSEVENTUAL) numTxnPerEpoch *= 2;

            // this worker will only generate transactions access those 2 silos (for the scale out experiment)
            var firstSiloID = 2 * (workerID / 2);
            var siloDist = new DiscreteUniform(firstSiloID, firstSiloID + 1, new Random());           // [firstSiloID, firstSiloID + 1]

            if (Constants.distribution == Distribution.UNIFORM || (Constants.distribution == Distribution.HOTRECORD && workload.grainSkewness == 1))
            {
                Console.WriteLine($"Generate workload for UNIFORM...");
                var grainDist = new DiscreteUniform(0, Constants.numGrainPerSilo - 1, new Random());  // [0, numGrainPerSilo - 1]
                for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
                {
                    for (int txn = 0; txn < numTxnPerEpoch; txn++)
                    {
                        var grainsPerTxn = new List<int>();
                        var numSiloAccess = SelectNumSilo(workload.txnSize);
                        Debug.Assert(numSiloAccess <= workload.txnSize);
                        var siloList = new List<int>();
                        for (int j = 0; j < numSiloAccess; j++)   // how many silos the txn will access
                        {
                            var silo = siloDist.Sample();
                            while (siloList.Contains(silo)) silo = siloDist.Sample();
                            siloList.Add(silo);
                        }
                        Debug.Assert(siloList.Count == numSiloAccess);

                        for (int k = 0; k < workload.txnSize; k++)
                        {
                            var silo = siloList[k % numSiloAccess];
                            var grainInSilo = grainDist.Sample();
                            var grainID = silo * Constants.numGrainPerSilo + grainInSilo;
                            while (grainsPerTxn.Contains(grainID))
                            {
                                grainInSilo = grainDist.Sample();
                                grainID = silo * Constants.numGrainPerSilo + grainInSilo;
                            }
                            grainsPerTxn.Add(grainID);
                        }
                        Debug.Assert(grainsPerTxn.Count == workload.txnSize);
                        shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(numSiloAccess > 1, grainsPerTxn)));
                    }
                }
            }
            else if (Constants.distribution == Distribution.HOTRECORD)
            {
                int numHotGrain = (int)(workload.grainSkewness * Constants.numGrainPerSilo);
                var numHotGrainPerTxn = Constants.txnSkewness * workload.txnSize;
                Console.WriteLine($"Generate workload for HOTRECORD, {numHotGrain} hot grains, {numHotGrainPerTxn} hot grain per txn...");
                var normal_dist = new DiscreteUniform(numHotGrain, Constants.numGrainPerSilo - 1, new Random());
                DiscreteUniform hot_dist = null;
                if (numHotGrain > 0) hot_dist = new DiscreteUniform(0, numHotGrain - 1, new Random());
                for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
                {
                    for (int txn = 0; txn < numTxnPerEpoch; txn++)
                    {
                        var grainsPerTxn = new List<int>();

                        // get the list of silos
                        var numSiloAccess = SelectNumSilo(workload.txnSize);
                        Debug.Assert(numSiloAccess <= workload.txnSize);
                        var siloList = new List<int>();
                        for (int j = 0; j < numSiloAccess; j++)   // how many silos the txn will access
                        {
                            var silo = siloDist.Sample();
                            while (siloList.Contains(silo)) silo = siloDist.Sample();
                            siloList.Add(silo);
                        }
                        Debug.Assert(siloList.Count == numSiloAccess);

                        for (int i = 0; i < workload.txnSize; i++)
                        {
                            if (i < numHotGrainPerTxn)   // those grains are selected from hot set
                            {
                                var silo = siloList[i % numSiloAccess];
                                var hotGrain = hot_dist.Sample() + silo * Constants.numGrainPerSilo;
                                while (grainsPerTxn.Contains(hotGrain)) hotGrain = hot_dist.Sample() + silo * Constants.numGrainPerSilo;
                                grainsPerTxn.Add(hotGrain);
                            }
                            else   // those grains are selected from non-hot set
                            {
                                var silo = siloList[i % numSiloAccess];
                                var normalGrain = normal_dist.Sample() + silo * Constants.numGrainPerSilo;
                                while (grainsPerTxn.Contains(normalGrain)) normalGrain = normal_dist.Sample() + silo * Constants.numGrainPerSilo;
                                grainsPerTxn.Add(normalGrain);
                            }
                        }

                        shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(numSiloAccess > 1, grainsPerTxn)));
                    }
                }
            }
            else
            {
                Debug.Assert(Constants.distribution == Distribution.ZIPFIAN);
                var zipf = Constants.zipfianConstant;
                Console.WriteLine($"Generate workload for ZIPFIAN, zipf = {zipf}");
                var prefix = Constants.dataPath + $@"MultiTransfer\{workload.txnSize}\zipf{zipf}_";

                // read data from files
                for (int epoch = 0; epoch < Constants.numEpoch; epoch++)
                {
                    string line;
                    var path = prefix + $@"epoch{epoch}.txt";
                    var file = new StreamReader(path);
                    while ((line = file.ReadLine()) != null)
                    {
                        var grainsPerTxn = new List<int>();
                        for (int i = 0; i < workload.txnSize; i++)
                        {
                            if (i > 0) line = file.ReadLine();  // the 0th line has been read by while() loop
                            var id = int.Parse(line);
                            grainsPerTxn.Add(id);
                        }
                        shared_requests[epoch].Enqueue(new Tuple<bool, RequestData>(isDet(), new RequestData(false, grainsPerTxn)));
                    }
                    file.Close();
                }
            }
        }
    }
}
