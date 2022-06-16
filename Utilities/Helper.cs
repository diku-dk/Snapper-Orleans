using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Utilities
{
    public static class Helper
    {
        // 1 ItemGrain + 1 WarehouseGrain + 10 DistrictGrain + 10 CustomerGrain + xx StockGrain + yy OrderGrain

        public static int GetItemGrain(int W_ID)
        {
            return W_ID * Constants.NUM_GRAIN_PER_W;
        }

        public static int GetWarehouseGrain(int W_ID)
        {
            return W_ID * Constants.NUM_GRAIN_PER_W + 1;
        }

        public static int GetDistrictGrain(int W_ID, int D_ID)
        {
            return W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + D_ID;
        }

        public static int GetCustomerGrain(int W_ID, int D_ID)
        {
            return W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 10 + D_ID;
        }

        public static int GetStockGrain(int W_ID, int I_ID)
        {
            return W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + I_ID / (Constants.NUM_I / Constants.NUM_StockGrain_PER_W);
        }

        public static int GetOrderGrain(int W_ID, int D_ID, int C_ID)
        {
            return W_ID * Constants.NUM_GRAIN_PER_W + 1 + 1 + 2 * Constants.NUM_D_PER_W + Constants.NUM_StockGrain_PER_W + D_ID * Constants.NUM_OrderGrain_PER_D + C_ID / (Constants.NUM_C_PER_D / Constants.NUM_OrderGrain_PER_D);
        }

        public static int MapGrainIDToServiceID(int grainID, int numService)
        {
            return grainID % numService;
        }

        public static int NURand(int A, int x, int y, int C)
        {
            var rnd = new Random();
            var part1 = rnd.Next(0, A + 1);
            var part2 = rnd.Next(x, y + 1);
            return (((part1 | part2) + C) % (y - x + 1)) + x;
        }

        public static void SetCPU(int processID, string processName, int numCPU)
        {
            Console.WriteLine($"Set processor affinity for {processName}...");
            var processes = Process.GetProcessesByName(processName);

            var str = GetSiloProcessorAffinity(processID, numCPU);
            var serverProcessorAffinity = Convert.ToInt64(str, 2);     // server uses the highest n bits

            processes[processID].ProcessorAffinity = (IntPtr)serverProcessorAffinity;
            Console.WriteLine($"Process affinity is set up for {processName}[{processID}]");
        }

        static string GetSiloProcessorAffinity(int processID, int numCPU)
        {
            var str = "";
            var firstCPUIndex = processID * Constants.numCPUPerSilo;
            for (int i = 0; i < Environment.ProcessorCount; i++)
            {
                if (i == firstCPUIndex)
                {
                    for (int j = 0; j < numCPU; j++) str += "1";
                    i += numCPU - 1;
                }
                else str += "0";
            }
            return str;
        }

        public static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
                if (ip.AddressFamily == AddressFamily.InterNetwork) return ip.ToString();

            throw new Exception("No network adapters with an IPv4 address in the system!");
        }

        public static string GetPublicIPAddress()
        {
            return new WebClient().DownloadString("https://ipv4.icanhazip.com/").TrimEnd();
        }

        public static string ChangeFormat(double n, int num)
        {
            if (n != double.NaN) return Math.Round(n, num).ToString().Replace(',', '.');
            else return "x";
        }
    }
}