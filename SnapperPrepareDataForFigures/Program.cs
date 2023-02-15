using System;
using System.Diagnostics;
using System.IO;
using Utilities;

namespace SnapperPrepareDataForFigures
{
    static class Program
    {
        static double[] f12_pact_tp = new double[6];
        static double[] f12_pact_delta = new double[6];
        static double[] f12_act_tp = new double[6];
        static double[] f12_act_delta = new double[6];
        static double[] f12_abort = new double[6];
        static int[] f12_nt_tp = new int[6];

        static int[] f14_tp_pact = new int[5];
        static int[] f14_tp_act = new int[5];
        static int[] f14_tp_orleans = new int[5];
        static int[] f14_tp_noDL = new int[5];
        static double[] f14_act_abort = new double[5];
        static double[] f14_orleans_abort = new double[5];

        static int[] f16a_act = new int[35];
        static int[] f16a_pact = new int[35];
        static double[] f16b_act_50 = new double[39];
        static double[] f16b_act_90 = new double[39];
        static double[] f16b_pact_50 = new double[39];
        static double[] f16b_pact_90 = new double[39];
        static double[] f16c_act_RW = new double[40];
        static double[] f16c_act_DL = new double[40];
        static double[] f16c_act_SE_not = new double[40];
        static double[] f16c_act_SE_sure = new double[40];

        static int[] f17a1_100pact_tp = new int[4];
        static int[] f17a1_100pact_sd = new int[4];
        static int[] f17a1_90pact_tp = new int[4];
        static int[] f17a1_90pact_sd = new int[4];
        static int[] f17a1_0pact_tp = new int[4];
        static int[] f17a1_0pact_sd = new int[4];
        static int[] f17a1_nt_tp = new int[4];

        static int[] f17a2_100pact_tp = new int[4];
        static int[] f17a2_100pact_sd = new int[4];
        static int[] f17a2_90pact_tp = new int[4];
        static int[] f17a2_90pact_sd = new int[4];
        static int[] f17a2_0pact_tp = new int[4];
        static int[] f17a2_0pact_sd = new int[4];
        static int[] f17a2_nt_tp = new int[4];

        static int[] f17b1_100pact_tp = new int[4];
        static int[] f17b1_100pact_sd = new int[4];
        static int[] f17b1_90pact_tp = new int[4];
        static int[] f17b1_90pact_sd = new int[4];
        static int[] f17b1_0pact_tp = new int[4];
        static int[] f17b1_0pact_sd = new int[4];
        static int[] f17b1_nt_tp = new int[4];

        static int[] f17b2_100pact_tp = new int[4];
        static int[] f17b2_100pact_sd = new int[4];
        static int[] f17b2_90pact_tp = new int[4];
        static int[] f17b2_90pact_sd = new int[4];
        static int[] f17b2_0pact_tp = new int[4];
        static int[] f17b2_0pact_sd = new int[4];
        static int[] f17b2_nt_tp = new int[4];

        static void Main()
        {
            try
            {
                using (var file = new StreamReader(Constants.resultPath))
                {
                    string line;
                    while ((line = file.ReadLine()) != null)
                    {
                        var strs = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                        switch (strs[0])
                        {
                            case "Fig.12":
                                PreProcessFig12Data(strs);
                                break;
                            case "Fig.14":
                                PreProcessFig14Data(strs);
                                break;
                            case "Fig.16":
                                PreProcessFig16Data(strs);
                                break;
                            case "Fig.17":
                                PreProcessFig17Data(strs);
                                break;
                            case "Fig.171":
                                PreProcessFig17Data(strs);
                                break;
                            case "Fig.172":
                                PreProcessFig17Data(strs);
                                break;
                            case "Fig.173":
                                PreProcessFig17Data(strs);
                                break;
                            default:
                                break;     // header line
                        }
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.Message} {e.StackTrace}");
                throw;
            }

            PostProcessFig12Data();

            var path = Constants.dataPath + "LoadDataForFigures.m";
            File.Delete(path);
            using (var file = new StreamWriter(path, true))
            {
                file.WriteLine("clear;");
                file.WriteLine();

                PrintDoubleData(file, f12_pact_tp, "f12_pact_tp", "% pact_tp (with logging)");
                PrintDoubleData(file, f12_pact_delta, "f12_pact_delta", "% delta_tp = no logging tp - with logging tp");
                PrintDoubleData(file, f12_act_tp, "f12_act_tp", "% act_tp (with logging)");
                PrintDoubleData(file, f12_act_delta, "f12_act_delta");
                PrintDoubleData(file, f12_abort, "f12_abort");
                PrintIntData(file, f12_nt_tp, "f12_nt_tp");
                file.WriteLine();

                PrintIntData(file, f14_tp_pact, "f14_tp_pact");
                PrintIntData(file, f14_tp_act, "f14_tp_act");
                PrintIntData(file, f14_tp_orleans, "f14_tp_orleans", "% OrleansTxn");
                PrintIntData(file, f14_tp_noDL, "f14_tp_noDL", "% OrleansTxn (no deadlock)");
                PrintDoubleData(file, f14_act_abort, "f14_act_abort");
                PrintDoubleData(file, f14_orleans_abort, "f14_orleans_abort");
                file.WriteLine();

                PrintIntData(file, f16a_act, "f16a_act");
                PrintIntData(file, f16a_pact, "f16a_pact");
                PrintDoubleData(file, f16b_act_50, "f16b_act_50");
                PrintDoubleData(file, f16b_act_90, "f16b_act_90");
                PrintDoubleData(file, f16b_pact_50, "f16b_pact_50");
                PrintDoubleData(file, f16b_pact_90, "f16b_pact_90");
                PrintDoubleData(file, f16c_act_RW, "f16c_act_RW");
                PrintDoubleData(file, f16c_act_DL, "f16c_act_DL");
                PrintDoubleData(file, f16c_act_SE_not, "f16c_act_SE_not");
                PrintDoubleData(file, f16c_act_SE_sure, "f16c_act_SE_sure");
                file.WriteLine();

                PrintIntData(file, f17a1_100pact_tp, "f17a1_100pact_tp");
                PrintIntData(file, f17a1_100pact_sd, "f17a1_100pact_sd");
                PrintIntData(file, f17a1_90pact_tp, "f17a1_90pact_tp");
                PrintIntData(file, f17a1_90pact_sd, "f17a1_90pact_sd");
                PrintIntData(file, f17a1_0pact_tp, "f17a1_0pact_tp");
                PrintIntData(file, f17a1_0pact_sd, "f17a1_0pact_sd");
                PrintIntData(file, f17a1_nt_tp, "f17a1_nt_tp");
                file.WriteLine();

                PrintIntData(file, f17a2_100pact_tp, "f17a2_100pact_tp");
                PrintIntData(file, f17a2_100pact_sd, "f17a2_100pact_sd");
                PrintIntData(file, f17a2_90pact_tp, "f17a2_90pact_tp");
                PrintIntData(file, f17a2_90pact_sd, "f17a2_90pact_sd");
                PrintIntData(file, f17a2_0pact_tp, "f17a2_0pact_tp");
                PrintIntData(file, f17a2_0pact_sd, "f17a2_0pact_sd");
                PrintIntData(file, f17a2_nt_tp, "f17a2_nt_tp");
                file.WriteLine();

                PrintIntData(file, f17b1_100pact_tp, "f17b1_100pact_tp");
                PrintIntData(file, f17b1_100pact_sd, "f17b1_100pact_sd");
                PrintIntData(file, f17b1_90pact_tp, "f17b1_90pact_tp");
                PrintIntData(file, f17b1_90pact_sd, "f17b1_90pact_sd");
                PrintIntData(file, f17b1_0pact_tp, "f17b1_0pact_tp");
                PrintIntData(file, f17b1_0pact_sd, "f17b1_0pact_sd");
                PrintIntData(file, f17b1_nt_tp, "f17b1_nt_tp");
                file.WriteLine();

                PrintIntData(file, f17b2_100pact_tp, "f17b2_100pact_tp");
                PrintIntData(file, f17b2_100pact_sd, "f17b2_100pact_sd");
                PrintIntData(file, f17b2_90pact_tp, "f17b2_90pact_tp");
                PrintIntData(file, f17b2_90pact_sd, "f17b2_90pact_sd");
                PrintIntData(file, f17b2_0pact_tp, "f17b2_0pact_tp");
                PrintIntData(file, f17b2_0pact_sd, "f17b2_0pact_sd");
                PrintIntData(file, f17b2_nt_tp, "f17b2_nt_tp");
                file.WriteLine();
            }
        }

        static void PrintIntData(StreamWriter file, int[] data, string name, string comment = "")
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

        static void PrintDoubleData(StreamWriter file, double[] data, string name, string comment = "")
        {
            file.Write($"{name} = [");
            for (int i = 0; i < data.Length; i++)
            {
                var str = double.IsNaN(data[i]) ? "NaN" : Math.Round(data[i], 4).ToString().Replace(',', '.');
                if (i < data.Length - 1) file.Write($"{str}, ");
                else
                {
                    file.Write($"{str}];     {comment}");
                    file.WriteLine();
                }
            }
        }

        static void PreProcessFig12Data(string[] strs)
        {
            var txnsize = int.Parse(strs[(int)ColumnName.txnSize]);
            var index = GetIndexOfTxnsize(txnsize);

            switch (strs[(int)ColumnName.implementation])
            {
                case "NONTXN":
                    f12_nt_tp[index] = int.Parse(strs[(int)ColumnName.act_tp]);
                    break;
                case "SNAPPER":
                    if (bool.Parse(strs[(int)ColumnName.loggingEnabled]))
                    {
                        if (strs[(int)ColumnName.pactPercent] == "100%")
                            f12_pact_tp[index] = double.Parse(strs[(int)ColumnName.pact_tp]);
                        else if (strs[(int)ColumnName.pactPercent] == "0%")
                        {
                            f12_act_tp[index] = double.Parse(strs[(int)ColumnName.act_tp]);
                            f12_abort[index] = double.Parse(strs[(int)ColumnName.total_abort_rate].Trim('%')) / 100.0;
                        }
                    }
                    else
                    {
                        if (strs[(int)ColumnName.pactPercent] == "100%")
                            f12_pact_delta[index] = double.Parse(strs[(int)ColumnName.pact_tp]);
                        else if (strs[(int)ColumnName.pactPercent] == "0%")
                            f12_act_delta[index] = double.Parse(strs[(int)ColumnName.act_tp]);
                    }
                    break;
                default:
                    throw new Exception($"Exception: Unknown implementation type {strs[(int)ColumnName.implementation]}");
            }
        }

        static void PostProcessFig12Data()
        {
            for (int i = 0; i < 6; i++)
            {
                f12_pact_delta[i] -= f12_pact_tp[i];      // delta = no logging tp - with logging tp
                f12_pact_delta[i] *= 1.0 / f12_nt_tp[i];  // relative tp = delta / nt tp
                Debug.Assert(f12_pact_delta[i] != 0);
                f12_pact_tp[i] *= 1.0 / f12_nt_tp[i];     // relative tp = tp / nt tp
                Debug.Assert(f12_pact_tp[i] != 0);

                f12_act_delta[i] -= f12_act_tp[i];        // delta = no logging tp - with logging tp
                f12_act_delta[i] *= 1.0 / f12_nt_tp[i];   // relative tp = delta / nt tp
                Debug.Assert(f12_act_delta[i] != 0);
                f12_act_tp[i] *= 1.0 / f12_nt_tp[i];      // relative tp = tp / nt tp
                Debug.Assert(f12_act_tp[i] != 0);
            }
        }

        static void PreProcessFig14Data(string[] strs)
        {
            var index = GetIndexOfZipf(strs[(int)ColumnName.zipfianConstant]);

            switch (strs[(int)ColumnName.implementation])
            {
                case "SNAPPER":
                    if (strs[(int)ColumnName.pactPercent] == "100%")
                        f14_tp_pact[index] = int.Parse(strs[(int)ColumnName.pact_tp]);
                    else if (strs[(int)ColumnName.pactPercent] == "0%")
                    {
                        f14_tp_act[index] = int.Parse(strs[(int)ColumnName.act_tp]);
                        f14_act_abort[index] = double.Parse(strs[(int)ColumnName.total_abort_rate].Trim('%')) / 100.0;
                    }
                    break;
                case "ORLEANSTXN":
                    if (bool.Parse(strs[(int)ColumnName.noDeadlock]))
                        f14_tp_noDL[index] = int.Parse(strs[(int)ColumnName.act_tp]);
                    else
                    {
                        f14_tp_orleans[index] = int.Parse(strs[(int)ColumnName.act_tp]);
                        f14_orleans_abort[index] = double.Parse(strs[(int)ColumnName.total_abort_rate].Trim('%')) / 100.0;
                    }
                    break;
                default:
                    throw new Exception($"Exception: Unknown implementation type {strs[(int)ColumnName.implementation]}");
            }
        }

        static void PreProcessFig16Data(string[] strs)
        {
            var group_index = GetIndexOfZipf(strs[(int)ColumnName.zipfianConstant]);
            var stack_index = GetIndexOfPactPercent(strs[(int)ColumnName.pactPercent]);

            var index1 = group_index * 7 + stack_index;
            f16a_act[index1] = int.Parse(strs[(int)ColumnName.act_tp]);
            f16a_pact[index1] = int.Parse(strs[(int)ColumnName.pact_tp]);

            var index2 = group_index * 8 + stack_index;
            f16b_act_50[index2] = double.Parse(strs[(int)ColumnName.act_50th_latency_ms]);
            f16b_act_90[index2] = double.Parse(strs[(int)ColumnName.act_90th_latency_ms]);
            f16b_pact_50[index2] = double.Parse(strs[(int)ColumnName.pact_50th_latency_ms]);
            f16b_pact_90[index2] = double.Parse(strs[(int)ColumnName.pact_90th_latency_ms]);

            var total_abort = double.Parse(strs[(int)ColumnName.total_abort_rate].Trim('%')) / 100.0;
            Debug.Assert(double.IsNaN(total_abort) || (total_abort >= 0 && total_abort <= 1));

            var total_act_tp = f16a_act[index1] * 1.0 / (1 - total_abort);
            var total_tp = total_act_tp + f16a_pact[index1];

            var rw = double.Parse(strs[(int)ColumnName.abortRWConflict].Trim('%')) / 100.0;
            Debug.Assert(double.IsNaN(rw) || (rw >= 0 && rw <= 1));
            var dl = double.Parse(strs[(int)ColumnName.abortDeadlock].Trim('%')) / 100.0;
            Debug.Assert(double.IsNaN(dl) || (dl >= 0 && dl <= 1));
            var se_not = double.Parse(strs[(int)ColumnName.abortNotSureSerializable].Trim('%')) / 100.0;
            Debug.Assert(double.IsNaN(se_not) || (se_not >= 0 && se_not <= 1));
            var se_sure = double.Parse(strs[(int)ColumnName.abortNotSerializable].Trim('%')) / 100.0;
            Debug.Assert(double.IsNaN(se_sure) || (se_sure >= 0 && se_sure <= 1));

            f16c_act_RW[index2] = total_abort * rw * total_act_tp / total_tp;
            f16c_act_DL[index2] = total_abort * dl * total_act_tp / total_tp;
            f16c_act_SE_not[index2] = total_abort * se_not * total_act_tp / total_tp;
            f16c_act_SE_sure[index2] = total_abort * se_sure * total_act_tp / total_tp;
        }

        static void PreProcessFig17Data(string[] strs)
        {
            var cpu = int.Parse(strs[(int)ColumnName.Silo_vCPU]);
            var index = GetIndexOfNumCPU(cpu);

            switch (strs[(int)ColumnName.implementation])
            {
                case "NONTXN":
                    switch (strs[(int)ColumnName.benchmark])
                    {
                        case "SMALLBANK":
                            switch (strs[(int)ColumnName.distribution])
                            {
                                case "UNIFORM":
                                    f17a1_nt_tp[index] = int.Parse(strs[(int)ColumnName.act_tp]) / 1000;
                                    break;
                                case "HOTSPOT":
                                    f17a2_nt_tp[index] = int.Parse(strs[(int)ColumnName.act_tp]) / 1000;
                                    break;
                                default:
                                    throw new Exception($"Exception: Unknown distribution {strs[(int)ColumnName.distribution]}");
                            }
                            break;
                        case "TPCC":
                            switch (strs[(int)ColumnName.NUM_OrderGrain_PER_D])
                            {
                                case "2":
                                    f17b1_nt_tp[index] = int.Parse(strs[(int)ColumnName.act_tp]) / 1000;
                                    break;
                                case "1":
                                    f17b2_nt_tp[index] = int.Parse(strs[(int)ColumnName.act_tp]) / 1000;
                                    break;
                                default:
                                    throw new Exception($"Exception: Unsupported NUM_OrderGrain_PER_D {strs[(int)ColumnName.NUM_OrderGrain_PER_D]}");
                            }
                            break;
                        default:
                            throw new Exception($"Exception: Unknown benchmark {strs[(int)ColumnName.benchmark]}");
                    }
                    break;
                case "SNAPPER":
                    var pactPercent = strs[(int)ColumnName.pactPercent];
                    switch (strs[(int)ColumnName.benchmark])
                    {
                        case "SMALLBANK":
                            switch (strs[(int)ColumnName.distribution])
                            {
                                case "UNIFORM":
                                    if (pactPercent == "100%")
                                    {
                                        f17a1_100pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17a1_100pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else if (pactPercent == "90%")
                                    {
                                        f17a1_90pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17a1_90pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else if (pactPercent == "0%")
                                    {
                                        f17a1_0pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17a1_0pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else throw new Exception($"Exception: Unsupported PactPercent {pactPercent}");
                                    break;
                                case "HOTSPOT":
                                    if (pactPercent == "100%")
                                    {
                                        f17a2_100pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17a2_100pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else if (pactPercent == "90%")
                                    {
                                        f17a2_90pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17a2_90pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else if (pactPercent == "0%")
                                    {
                                        f17a2_0pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17a2_0pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else throw new Exception($"Exception: Unsupported PactPercent {pactPercent}");
                                    break;
                                default:
                                    throw new Exception($"Exception: Unknown distribution {strs[(int)ColumnName.distribution]}");
                            }
                            break;
                        case "TPCC":
                            switch (strs[(int)ColumnName.NUM_OrderGrain_PER_D])
                            {
                                case "2":
                                    if (pactPercent == "100%")
                                    {
                                        f17b1_100pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17b1_100pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else if (pactPercent == "90%")
                                    {
                                        f17b1_90pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17b1_90pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else if (pactPercent == "0%")
                                    {
                                        f17b1_0pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17b1_0pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else throw new Exception($"Exception: Unsupported PactPercent {pactPercent}");
                                    break;
                                case "1":
                                    if (pactPercent == "100%")
                                    {
                                        f17b2_100pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17b2_100pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else if (pactPercent == "90%")
                                    {
                                        f17b2_90pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17b2_90pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else if (pactPercent == "0%")
                                    {
                                        f17b2_0pact_tp[index] = int.Parse(strs[(int)ColumnName.pact_tp]) + int.Parse(strs[(int)ColumnName.act_tp]);
                                        f17b2_0pact_sd[index] = int.Parse(strs[(int)ColumnName.pact_sd]) + int.Parse(strs[(int)ColumnName.act_sd]);
                                    }
                                    else throw new Exception($"Exception: Unsupported PactPercent {pactPercent}");
                                    break;
                                default:
                                    throw new Exception($"Exception: Unsupported NUM_OrderGrain_PER_D {strs[(int)ColumnName.NUM_OrderGrain_PER_D]}");
                            }
                            break;
                        default:
                            throw new Exception($"Exception: Unknown benchmark {strs[(int)ColumnName.benchmark]}");
                    }
                    break;
                default:
                    throw new Exception($"Exception: Unknown implementation type {strs[(int)ColumnName.implementation]}");
            }
        }

        static int GetIndexOfTxnsize(int txnsize)
        {
            switch (txnsize)
            {
                case 2:
                    return 0;
                case 4:
                    return 1;
                case 8:
                    return 2;
                case 16:
                    return 3;
                case 32:
                    return 4;
                case 64:
                    return 5;
                default:
                    throw new Exception($"Exception: Unsupported txnsize {txnsize}");
            }
        }

        static int GetIndexOfZipf(string zipfianConstant)
        {
            switch (zipfianConstant)
            {
                case "0":
                    return 0;
                case "0.9":
                    return 1;
                case "1":
                    return 2;
                case "1.25":
                    return 3;
                case "1.5":
                    return 4;
                default:
                    throw new Exception($"Exception: Unsupported zipfianConstant {zipfianConstant}");
            }
        }

        static int GetIndexOfPactPercent(string percent)
        {
            switch (percent)
            {
                case "100%":
                    return 0;
                case "99%":
                    return 1;
                case "90%":
                    return 2;
                case "75%":
                    return 3;
                case "50%":
                    return 4;
                case "25%":
                    return 5;
                case "0%":
                    return 6;
                default:
                    throw new Exception($"Exception: Unsupported PactPercent {percent}");
            }
        }

        static int GetIndexOfNumCPU(int cpu)
        {
            switch (cpu)
            {
                case 4:
                    return 0;
                case 8:
                    return 1;
                case 16:
                    return 2;
                case 32:
                    return 3;
                default:
                    throw new Exception($"Exception: Unsupported cpu {cpu}");
            }
        }
    }

    public enum ColumnName
    {
        Fig,
        Silo_vCPU,
        implementation,
        benchmark,
        loggingEnabled,
        NUM_OrderGrain_PER_D,
        pactPercent,
        txnSize,
        numWriter,
        distribution,
        zipfianConstant,
        actPipeSize,
        pactPipeSize,
        noDeadlock,
        pact_tp,
        pact_sd,
        act_tp,
        act_sd,
        total_abort_rate,
        abortRWConflict,
        abortDeadlock,
        abortNotSureSerializable,
        abortNotSerializable,
        pact_50th_latency_ms,
        pact_90th_latency_ms,
        pact_99th_latency_ms,
        act_50th_latency_ms,
        act_90th_latency_ms,
        act_99th_latency_ms
    };
}