using System;
using System.IO;
using System.Threading;
using System.Collections.Generic;
using Utilities;
using MathNet.Numerics.Distributions;

namespace SmallBank.DataGenerator
{
    class Program
    {
        static int numCPUPerSilo = 4;
        static int txnSize = 4;
        static int numEpoch = 6;
        static double[] zip = { 0.9, 1.0, 1.25, 1.5 };
        static int numTxnPerEpoch = 100000;

        static void ThreadWork(object obj)
        {
            var tuple = (Tuple<double, int>)obj;
            var zipf = tuple.Item1;
            var epoch = tuple.Item2;
            
            Console.WriteLine($"Gnerate workload: zipf = {zipf}, epoch = {epoch}");
            var numGrainPerSilo = Helper.GetNumGrainPerSilo(numCPUPerSilo);
            var distribution = new Zipf(zipf, numGrainPerSilo, new Random());
            var filePath = Constants.dataPath + $@"zipfian_workload\zipf{zipf}_epoch{epoch}.txt";
            using (var file = new StreamWriter(filePath, true))
            {
                for (int i = 0; i < numTxnPerEpoch; i++)
                {
                    var list = new HashSet<int>();
                    for (int j = 0; j < txnSize; j++)
                    {
                        var grainID = distribution.Sample();
                        while (list.Contains(grainID)) grainID = distribution.Sample();
                        list.Add(grainID);
                        file.Write($"{grainID - 1} ");
                    }
                    file.WriteLine();
                }
            }
        }

        static void Main()
        {
            var directoryPath = Constants.dataPath + $@"zipfian_workload";
            Directory.CreateDirectory(directoryPath);
            for (int j = 0; j < zip.Length; j++)
            {
                var zipf = zip[j];
                for (int epoch = 0; epoch < numEpoch; epoch++)
                {
                    var thread = new Thread(ThreadWork);
                    thread.Start(new Tuple<double, int>(zipf, epoch));
                }
            }
        }
    }
}