using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using Utilities;
using MathNet.Numerics.Statistics;

namespace PrepareDataForFigures
{
    static class Program
    {
        static double[] f15_orleans_4W0N = new double[Constants.numIntervals];
        static double[] f15_orleans_1W3N = new double[Constants.numIntervals];
        static double[] f15_orleans_0W4N = new double[Constants.numIntervals];
        static double[] f15_orleans_0W1N = new double[Constants.numIntervals];
        static double[] f15_act_4W0N = new double[Constants.numIntervals];
        static double[] f15_act_1W3N = new double[Constants.numIntervals];
        static double[] f15_act_0W4N = new double[Constants.numIntervals];
        static double[] f15_act_0W1N = new double[Constants.numIntervals];

        static ImplementationType implementation;
        static int numWriter = -1;
        static int numNoOp = -1;

        static List<double[]> interval = new List<double[]>();

        static void Main()
        {
            try
            {
                using (var file = new StreamReader(Constants.latencyPath))
                {
                    var recordNumber = -1;
                    string line;
                    while ((line = file.ReadLine()) != null)
                    {
                        var strs = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        if (strs[(int)ColumnName.Fig] == "Fig") continue;      // the header line
                        if (strs[(int)ColumnName.Fig] == "Fig.15")
                        {
                            // if this is not the first group of data, need to calculate the mean value for the previous group now
                            if (numWriter != -1) ProcessData();
                            
                            implementation = Enum.Parse<ImplementationType>(strs[(int)ColumnName.implementation]);
                            var txnSize = int.Parse(strs[(int)ColumnName.txnSize]);
                            numWriter = int.Parse(strs[(int)ColumnName.numWriter]);
                            numNoOp = txnSize - numWriter;
                            recordNumber = 0;
                            continue;
                        }

                        // load data from file
                        Debug.Assert(strs.Length == Constants.numIntervals && recordNumber != -1);
                        var item = new double[Constants.numIntervals];
                        for (int i = 0; i < Constants.numIntervals; i++)
                            item[i] = double.Parse(strs[i]);
                        interval.Add(item);
                        recordNumber++;
                    }
                }

                // calculate the mean value for the last group of data
                ProcessData();

                // print data
                var path = Constants.dataPath + "LoadDataForFigures.m";
                File.Delete(path);
                using (var file = new StreamWriter(path, true))
                {
                    file.WriteLine("clear;");
                    file.WriteLine();

                    PrintDoubleData(file, f15_orleans_4W0N, "f15_orleans_4W0N");
                    PrintDoubleData(file, f15_orleans_1W3N, "f15_orleans_1W3N");
                    PrintDoubleData(file, f15_orleans_0W4N, "f15_orleans_0W4N");
                    PrintDoubleData(file, f15_orleans_0W1N, "f15_orleans_0W1N");

                    PrintDoubleData(file, f15_act_4W0N, "f15_act_4W0N");
                    PrintDoubleData(file, f15_act_1W3N, "f15_act_1W3N");
                    PrintDoubleData(file, f15_act_0W4N, "f15_act_0W4N");
                    PrintDoubleData(file, f15_act_0W1N, "f15_act_0W1N");
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.Message} {e.StackTrace}");
                throw;
            }
        }

        static void ProcessData()
        {
            Console.WriteLine($"Process data for {implementation}, numWriter = {numWriter}, numNoOp = {numNoOp}");
            RemoveAbnormalData();
            ValidateSD();

            CalculateAverage();
            interval = new List<double[]>();
        }

        static double[] GetColumn(int index)
        {
            var column = new double[interval.Count];
            for (int j = 0; j < interval.Count; j++) column[j] = interval[j][index];
            return column;
        }

        static int ValidateSD()
        {
            var numInvalidColumn = 0;
            for (int i = 0; i < Constants.numIntervals; i++)
            {
                var meanAndSd = ArrayStatistics.MeanStandardDeviation(GetColumn(i));
                if (meanAndSd.Item2 > meanAndSd.Item1 * 0.25) numInvalidColumn++;
            }
            Console.WriteLine($"Number of invalid column = {numInvalidColumn}");
            return numInvalidColumn;
        }

        static void RemoveAbnormalData()
        {
            var count = interval.Count;
            Debug.Assert(count != 0);
            int numRemovedRecord = (int)(count * 0.01);    // remove 1% largest data from each column

            for (int i = 0; i < Constants.numIntervals; i++)
            {
                // sort according to the ith column
                interval = interval.OrderByDescending(x => x[i]).ToList();

                for (int j = 0; j < numRemovedRecord; j++) interval.RemoveAt(0);
            }

            var rate = (numRemovedRecord * Constants.numIntervals) * 100.0 / count;
            Console.WriteLine($"Removed {Math.Round(rate, 2).ToString().Replace(',', '.')}% records as total. ");
        }

        static void CalculateAverage()
        {
            switch (implementation)
            {
                case ImplementationType.SNAPPER:
                    switch (numWriter)
                    {
                        case 0:
                            switch (numNoOp)
                            {
                                case 1:
                                    SetIntervals(f15_act_0W1N);
                                    break;
                                case 4:
                                    SetIntervals(f15_act_0W4N);
                                    break;
                                default:
                                    throw new Exception($"Exception: Unsupported txn with numNoOp = {numNoOp}");
                            }
                            break;
                        case 1:
                            Debug.Assert(numNoOp == 3);
                            SetIntervals(f15_act_1W3N);
                            break;
                        case 4:
                            Debug.Assert(numNoOp == 0);
                            SetIntervals(f15_act_4W0N);
                            break;
                        default:
                            throw new Exception($"Exception: Unsupported txn with numWriter = {numWriter}");
                    }
                    break;
                case ImplementationType.ORLEANSTXN:
                    switch (numWriter)
                    {
                        case 0:
                            switch (numNoOp)
                            {
                                case 1:
                                    SetIntervals(f15_orleans_0W1N);
                                    break;
                                case 4:
                                    SetIntervals(f15_orleans_0W4N);
                                    break;
                                default:
                                    throw new Exception($"Exception: Unsupported txn with numNoOp = {numNoOp}");
                            }
                            break;
                        case 1:
                            Debug.Assert(numNoOp == 3);
                            SetIntervals(f15_orleans_1W3N);
                            break;
                        case 4:
                            Debug.Assert(numNoOp == 0);
                            SetIntervals(f15_orleans_4W0N);
                            break;
                        default:
                            throw new Exception($"Exception: Unsupported txn with numWriter = {numWriter}");
                    }
                    break;
                default:
                    throw new Exception($"Exception: Unsupported implementation {implementation}");
            }
        }

        static void SetIntervals(double[] target)
        {
            for (int i = 0; i < Constants.numIntervals; i++)
                target[i] = GetColumn(i).Mean();
        }

        static void PrintDoubleData(StreamWriter file, double[] data, string name, string comment = "")
        {
            file.Write($"{name} = [");
            for (int i = 0; i < data.Length; i++)
            {
                if (i < data.Length - 1) file.Write($"{data[i]}, ");
                else
                {
                    file.Write($"{data[i]}];     {comment}");
                    file.WriteLine();
                }
            }
        }
    }

    public enum ColumnName
    {
        Fig,
        Silo_vCPU,
        implementation,
        txnSize,
        numWriter,
        distribution,
        latency_50th_ms,
        breakdown_latency_I1_I9
    };
}