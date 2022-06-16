using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Utilities
{
    public static class Helper
    {
        static string UseHighestProcessors(int numCPUPerSilo)
        {
            var str = "";
            var numCPU = Environment.ProcessorCount;
            for (int i = 0; i < numCPU; i++)
            {
                if (i < numCPUPerSilo) str += "1";    // use the highest n bits
                else str += "0";
            }
            return str;
        }

        static string UseLowestProcessors(int numCPUPerSilo)
        {
            var str = "";
            var numCPU = Environment.ProcessorCount;
            for (int i = 0; i < numCPU; i++)
            {
                if (i >= numCPU - numCPUPerSilo) str += "1";    // use the lowest n bits
                else str += "0";
            }
            return str;
        }

        public static string GetWorkerProcessorAffinity(int numCPUPerSilo)
        {
            if (Constants.LocalCluster)
            {
                Debug.Assert(Environment.ProcessorCount >= 2 * numCPUPerSilo);
                return UseLowestProcessors(numCPUPerSilo);
            }
            else
            {
                Debug.Assert(Environment.ProcessorCount >= numCPUPerSilo);
                return UseHighestProcessors(numCPUPerSilo);
            } 
        }

        public static string GetSiloProcessorAffinity(int numCPUPerSilo)
        {
            if (Constants.LocalCluster) Debug.Assert(Environment.ProcessorCount >= 2 * numCPUPerSilo);
            else Debug.Assert(Environment.ProcessorCount >= numCPUPerSilo);

            return UseHighestProcessors(numCPUPerSilo);
        }

        public static int GetNumCoordPerSilo(int numCPUPerSilo)
        {
            return numCPUPerSilo / Constants.numCPUBasic * 8;
        }

        public static int GetNumPersistItemPerSilo(int numCPUPerSilo)
        {
            return numCPUPerSilo / Constants.numCPUBasic * 8;
        }

        public static int GetNumGrainPerSilo(int numCPUPerSilo)
        {
            return 10000 * numCPUPerSilo / Constants.numCPUBasic;
        }

        public static int GetNumWarehousePerSilo(int numCPUPerSilo)
        {
            return 2 * numCPUPerSilo / Constants.numCPUBasic;
        }

        public static int MapGrainIDToPersistItemID(int numPersistItem, int grainID)
        {
            return grainID % numPersistItem;
        }

        public static int NURand(int A, int x, int y, int C)
        {
            var rnd = new Random();
            var part1 = rnd.Next(0, A + 1);
            var part2 = rnd.Next(x, y + 1);
            return (((part1 | part2) + C) % (y - x + 1)) + x;
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
    }
}