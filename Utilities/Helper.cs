using System;
using System.Net;
using System.Net.Sockets;

namespace Utilities
{
    public static class Helper
    {
        public static int GetStockGrain(int W_ID, int I_ID)
        {
            return W_ID * Constants.NUM_StockGrain_PER_W + I_ID % Constants.NUM_StockGrain_PER_W;
        }

        public static int GetOrderGrain(int W_ID, int D_ID, int C_ID)
        {
            return W_ID * Constants.NUM_D_PER_W * Constants.NUM_OrderGrain_PER_D + D_ID * Constants.NUM_OrderGrain_PER_D + C_ID % Constants.NUM_OrderGrain_PER_D;
        }

        public static int MapGrainIDToPersistItemID(int numPersistItem, int grainID)
        {
            return grainID % numPersistItem;
        }

        public static int MapGrainIDToCoordID(int numCoord, int grainID)
        {
            return grainID % numCoord;
        }

        public static int NURand(int A, int x, int y, int C)
        {
            var rnd = new Random();
            var part1 = rnd.Next(0, A + 1);
            var part2 = rnd.Next(x, y + 1);
            return (((part1 | part2) + C) % (y - x + 1)) + x;
        }

        public static int GetSiloNumber(long coordID)
        {
            return (int)coordID / Constants.numCoordPerSilo;
        }

        public static int GetSiloNumber(long grainID, int siloLen)
        {
            var numCoord = siloLen * Constants.numCoordPerSilo;
            var coordID = grainID % numCoord;
            return (int)coordID / Constants.numCoordPerSilo;
        }

        public static bool intraSilo(int numCoord, int source, bool isSourceCoord, int dest, bool isDestCoord)
        {
            int sourceSilo;
            int destSilo;
            if (isSourceCoord) sourceSilo = source / Constants.numCoordPerSilo;
            else sourceSilo = (source % numCoord) / Constants.numCoordPerSilo;

            if (isDestCoord) destSilo = dest / Constants.numCoordPerSilo;
            else destSilo = (dest % numCoord) / Constants.numCoordPerSilo;

            return sourceSilo == destSilo;
        }

        public static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList) if (ip.AddressFamily == AddressFamily.InterNetwork) return ip.ToString();
            throw new Exception("No network adapters with an IPv4 address in the system!");
        }

        /*
        public struct Gr : IEquatable<GrainID>
        {
            public readonly int id;
            public readonly string name;

            public GrainID(int id, string name)
            {
                this.id = id;
                this.name = name;
            }

            public bool Equals(GrainID other)
            {
                return other.id == id && other.name == name;
            }
        }*/
    }
}
