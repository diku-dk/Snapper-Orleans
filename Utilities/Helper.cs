using System;
using System.Net;
using System.Net.Sockets;

namespace Utilities
{
    public static class Helper
    {
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
