using System;

namespace Utilities
{
    public static class GlobalCoordGrainPlacementHelper
    {
        public static int MapCoordIDToNeighborID(int coordID)
        {
            return (coordID + 1) % Constants.numGlobalCoord;
        }
    }

    public static class LocalConfigGrainPlacementHelper
    {
        public static int MapGrainIDToSilo(int grainID)
        {
            return grainID;   // one local config grain per silo
        }
    }

    public static class LocalCoordGrainPlacementHelper
    {
        static Random rnd = new Random();

        public static int MapCoordIDToSiloID(int coordID)
        {
            return coordID / Constants.numLocalCoordPerSilo;   // local coord [0, 7] locate in 0th silo
        }

        public static int MapSiloIDToFirstLocalCoordID(int siloID)   // find the first local coord in this silo
        {
            return siloID * Constants.numLocalCoordPerSilo;
        }

        public static int MapCoordIDToNeighborID(int coordID)
        {
            var siloID = MapCoordIDToSiloID(coordID);
            return (coordID + 1) % Constants.numLocalCoordPerSilo + siloID * Constants.numLocalCoordPerSilo;
        }

        public static int MapCoordIndexToCoordID(int index, int siloID)
        {
            return index + siloID * Constants.numLocalCoordPerSilo;
        }

        public static int MapSiloIDToRandomCoordID(int siloID)
        {
            var index = rnd.Next(0, Constants.numLocalCoordPerSilo);
            return MapCoordIndexToCoordID(index, siloID);
        }
    }

    public static class TransactionExecutionGrainPlacementHelper
    {
        public static int MapGrainIDToSilo(int grainID)
        {
            // grain [0, 10K) locate in the 0th silo
            return grainID / Constants.numGrainPerSilo;
        }
    }
}
