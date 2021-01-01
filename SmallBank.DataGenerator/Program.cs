using System;
using System.IO;
using System.Threading;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;

namespace SmallBank.DataGenerator
{
    class Program
    {
        private static int numEpoch = 6;
        private static int numGrain = 10000;
        private static int[] numGrainPerTxn = { 2 };
        private static double[] zip = { 1 };

        private static void ThreadWorkForHybrid(Object obj)
        {
            var tuple = (Tuple<int, double, int>)obj;
            var txnSize = tuple.Item1;
            var zipf = tuple.Item2;
            var epoch = tuple.Item3;
            var numTxn = 100000 * 4 / txnSize;

            var distribution = new Zipf(zipf, numGrain - 1, new Random());
            var filePath = $@"C:\Users\Administrator\Desktop\data\MultiTransfer\{txnSize}\zipf{zipf}_epoch{epoch}.txt";
            Console.WriteLine($"txnSize = {txnSize}, zipf = {zipf}, epoch = {epoch}");
            using (var file = new StreamWriter(filePath, true))
            {
                for (int j = 0; j < numTxn; j++)
                {
                    var list = new HashSet<int>();
                    for (int k = 0; k < txnSize; k++)
                    {
                        var grainID = distribution.Sample();
                        while (list.Contains(grainID)) grainID = distribution.Sample();
                        list.Add(grainID);
                        file.WriteLine($"{grainID}");
                    }
                }
            }
        }

        static void Main()
        {
            for (int i = 0; i < numGrainPerTxn.Length; i++)
            {
                var txnSize = numGrainPerTxn[i];
                for (int j = 0; j < zip.Length; j++)
                {
                    var zipf = zip[j];
                    for (int epoch = 0; epoch < numEpoch; epoch++)
                    {
                        var thread = new Thread(ThreadWorkForHybrid);
                        thread.Start(new Tuple<int, double, int>(txnSize, zipf, epoch));
                    }
                }
            }
        }
    }
}
