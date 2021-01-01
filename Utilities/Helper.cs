using System;
using System.Net;
using System.Net.Sockets;

namespace Utilities
{
    public static class Helper
    {
        public static int GetGrainID(int W_ID, int id, bool isDist)
        {
            int D_ID;
            if (isDist) D_ID = id;
            else D_ID = id % Constants.NUM_D_PER_W;
            return W_ID * Constants.NUM_D_PER_W + D_ID;
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
    }
}
