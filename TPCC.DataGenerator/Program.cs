using System;
using System.IO;
using Utilities;
using NewProcess;
using System.Linq;
using TPCC.Grains;
using System.Threading;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;

namespace TPCC.DataGenerator
{
    class Program
    {
        static bool generateWorkload = true;

        static int[] vCPU = { 4 };
        static Random random = new Random();
        const string numbers = "0123456789";
        const string alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        
        private static void GenerateWarehouseData(object obj)
        {
            var tuple = (Tuple<int, int, int>)obj;
            var W_ID = tuple.Item1;
            var D_ID = tuple.Item2;
            Console.WriteLine($"Generate data for W {W_ID}, D {D_ID}");
            var data = new WarehouseData();

            // generate data for Warehouse table
            var W_NAME = RandomString(1, alphanumeric);
            var W_STREET_1 = RandomString(1, alphanumeric);
            var W_STREET_2 = RandomString(1, alphanumeric);
            var W_CITY = RandomString(1, alphanumeric);
            var W_STATE = RandomString(2, alphanumeric);
            var W_ZIP = RandomString(9, alphanumeric);
            var W_TAX = numeric(4, 4, true);
            var W_YTD = numeric(12, 2, true);
            data.warehouse_info = new Warehouse(W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD);

            // generate data for District table
            string D_NAME = RandomString(1, alphanumeric);
            var D_STREET_1 = RandomString(1, alphanumeric);
            var D_STREET_2 = RandomString(1, alphanumeric);
            var D_CITY = RandomString(1, alphanumeric);
            var D_STATE = RandomString(2, alphanumeric);
            var D_ZIP = RandomString(9, alphanumeric);
            var D_TAX = numeric(4, 4, true);
            var D_YTD = numeric(12, 2, true);
            var D_NEXT_O_ID = random.Next(0, 10000000);
            data.district_info = new District(D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID);

            // generate data for Customer table
            for (int i = 0; i < Constants.NUM_C_PER_D; i++)
            {
                var C_ID = i;
                var C_FIRST = RandomString(1, alphanumeric);
                var C_MIDDLE = RandomString(2, alphanumeric);
                var C_LAST = RandomString(1, alphanumeric);
                var C_STREET_1 = RandomString(1, alphanumeric);
                var C_STREET_2 = RandomString(1, alphanumeric);
                var C_CITY = RandomString(1, alphanumeric);
                var C_STATE = RandomString(2, alphanumeric);
                var C_ZIP = RandomString(9, alphanumeric);
                var C_PHONE = RandomString(16, alphanumeric);
                var C_SINCE = DateTime.Now;
                var C_CREDIT = RandomString(2, alphanumeric);
                var C_CREDIT_LIM = numeric(12, 2, true);
                var C_DISCOUNT = numeric(4, 4, true);
                var C_BALANCE = numeric(12, 2, true);
                var C_YTD_PAYMENT = numeric(12, 2, true);
                var C_PAYMENT_CNT = numeric(4, false);
                var C_DELIVERY_CNT = numeric(4, false);
                var C_DATA = RandomString(1, alphanumeric);
                data.customer_table.Add(C_ID, new Customer(C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA));
            }

            // generate data for Item table
            var NUM_I_PER_D = Constants.NUM_I / Constants.NUM_D_PER_W;
            for (int i = 0; i < NUM_I_PER_D; i++)
            {
                var I_ID = i * Constants.NUM_D_PER_W + D_ID;
                var I_IM_ID = I_ID;
                var I_NAME = RandomString(1, alphanumeric);
                var I_PRICE = numeric(5, 2, false);
                var I_DATA = RandomString(1, alphanumeric);
                data.item_table.Add(I_ID, new Item(I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA));
            }

            // generate data for Stock table
            for (int i = 0; i < NUM_I_PER_D; i++)
            {
                var S_I_ID = i * Constants.NUM_D_PER_W + D_ID;
                var S_QUANTITY = numeric(4, true);
                var S_DIST = new Dictionary<int, string>();
                for (int d = 0; d < Constants.NUM_D_PER_W; d++) S_DIST.Add(d, RandomString(24, alphanumeric));
                var S_YTD = numeric(8, false);
                var S_ORDER_CNT = numeric(4, false);
                var S_REMOTE_CNT = numeric(4, false);
                var S_DATA = RandomString(1, alphanumeric);
                data.stock_table.Add(S_I_ID, new Stock(S_I_ID, S_QUANTITY, S_DIST, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA));
            }

            var grainID = Helper.GetGrainID(W_ID, D_ID, true);
            var filePath = Constants.TPCC_dataPath + $"{grainID}";
            var serializer = new MsgPackSerializer();
            File.WriteAllBytes(filePath, serializer.serialize(data));
        }

        private static string RandomString(int length, string chars)
        {
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private static int numeric(int m, bool signed)
        {
            var num  = random.Next((int)Math.Pow(10, m), (int)Math.Pow(10, m + 1));
            var isPositive = random.Next(0, 2);
            if (signed && isPositive > 1) return -num;
            else return num;
        }

        private static float numeric(int m, int n, bool signed)   // float 6-9位小数，double 15-17位小数
        {
            float the_number;
            var str = RandomString(m, numbers);
            if (m == n) the_number = float.Parse("0." + str);
            else if (m > n)
            {
                var left = str.Substring(0, m - n);
                var right = str.Substring(m - n);
                the_number = float.Parse(left + "." + right);
            }
            else
            {
                var left = "0.";
                for (int i = 0; i < n - m; i++) left += "0";
                the_number = float.Parse(left + str);
            }
            var isPositive = random.Next(0, 2);
            if (signed && isPositive > 0) return -the_number;
            return the_number;
        }

        static Random C_rnd = new Random();
        static IDiscreteDistribution district_dist_uni = new DiscreteUniform(0, Constants.NUM_D_PER_W - 1, new Random());
        static IDiscreteDistribution ol_cnt_dist_uni = new DiscreteUniform(5, 15, new Random());
        static IDiscreteDistribution rbk_dist_uni = new DiscreteUniform(1, 100, new Random());
        static IDiscreteDistribution local_dist_uni = new DiscreteUniform(1, 100, new Random());
        static IDiscreteDistribution quantity_dist_uni = new DiscreteUniform(1, 10, new Random());

        private static void GenerateWorkload(object obj)
        {
            var tuple = (Tuple<double, double, int, int, int>)obj;
            var skewness = tuple.Item1;
            var hotRatio = tuple.Item2;
            var num_wh = tuple.Item3;
            var num_txn = tuple.Item4;
            var epoch = tuple.Item5;

            var reqs = new List<RequestData>();
            for (int txn = 0; txn < num_txn; txn++)
            {
                var num_hot_wh = (int)(skewness * num_wh);
                var wh_dist_normal = new DiscreteUniform(num_hot_wh, num_wh - 1, new Random());
                IDiscreteDistribution wh_dist_hot = null;
                if (num_hot_wh > 0) wh_dist_hot = new DiscreteUniform(0, num_hot_wh - 1, new Random());

                // generate W_ID (75% possibility to be hot warehouse)
                int W_ID;
                var rnd = new Random();
                if (rnd.Next(0, 100) < hotRatio * 100 && num_hot_wh > 0) W_ID = wh_dist_hot.Sample();
                else W_ID = wh_dist_normal.Sample();

                var grains = new List<int>();
                var D_ID = district_dist_uni.Sample();
                grains.Add(W_ID * Constants.NUM_D_PER_W + D_ID);
                var C_ID = Helper.NURand(1023, 1, Constants.NUM_C_PER_D, C_rnd.Next(0, 1024));
                var ol_cnt = ol_cnt_dist_uni.Sample();
                var rbk = rbk_dist_uni.Sample();
                var itemsToBuy = new Dictionary<int, Tuple<int, int>>();  // <I_ID, <supply_warehouse, quantity>>
                for (int i = 0; i < ol_cnt; i++)
                {
                    int I_ID;
                    if (i == ol_cnt - 1 && rbk == 1) I_ID = -1;   // generate 1% of error
                    else
                    {
                        do I_ID = Helper.NURand(8191, 1, Constants.NUM_I, C_rnd.Next(0, 8192));
                        while (!itemsToBuy.ContainsKey(I_ID));
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
                reqs.Add(req);
            }
            var filePath = Constants.TPCC_workloadPath + $"{epoch}";
            var serializer = new MsgPackSerializer();
            File.WriteAllBytes(filePath, serializer.serialize(reqs));
        }

        static void Main()
        {
            if (generateWorkload)
            {
                // generate workload
                var numEpochs = 6;
                var skewness = 0;    // uniform
                var hotRatio = 0.75;

                for (int i = 0; i < vCPU.Length; i++)
                {
                    var cpu = vCPU[i];
                    var numTxnPerEpoch = 100000 * cpu / 4;
                    var numWarehouse = (int)(cpu * Constants.NUM_W_PER_4CORE / 4);
                    var wh_dist_uni = new DiscreteUniform(0, numWarehouse - 1, new Random());
                    for (int epoch = 0; epoch < numEpochs; epoch++)
                    {
                        var thread = new Thread(GenerateWorkload);
                        thread.Start(new Tuple<double, double, int, int, int>(skewness, hotRatio, numWarehouse, numTxnPerEpoch, epoch));
                    }
                }
            }
            else
            {
                // generate warehouse grain data
                for (int i = 0; i < vCPU.Length; i++)
                {
                    var CPU = vCPU[i];
                    var numWarehouse = CPU / 4 * 1000;
                    for (int w = 0; w < numWarehouse; w++)
                    {
                        for (int d = 0; d < Constants.NUM_D_PER_W; d++)
                        {
                            var thread = new Thread(GenerateWarehouseData);
                            thread.Start(new Tuple<int, int, int>(w, d, numWarehouse));
                        }
                    }
                }
            }
        }
    }
}
