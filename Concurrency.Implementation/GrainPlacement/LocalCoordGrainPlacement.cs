using System;
using Utilities;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class LocalCoordGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            var silo = 0;
            if (Constants.multiSilo)
            {
                var coordID = (int)target.GrainIdentity.PrimaryKeyLong;
                silo = LocalCoordGrainPlacementHelper.MapCoordIDToSiloID(coordID);
            }
            return Task.FromResult(silos[silo]);
        }
    }

    [Serializable]
    public class LocalCoordGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class LocalCoordGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public LocalCoordGrainPlacementStrategyAttribute() : base(new LocalCoordGrainPlacementStrategy())
        {
        }
    }

    public static class LocalCoordGrainPlacementHelper
    {
        public static int MapCoordIDToSiloID(int coordID)
        {
            return coordID / Constants.numLocalCoordPerSilo;   // local coord [0, 7] locate in 0th silo
        }

        public static int MapSiloIDToFirstCoordID(int siloID)   // fidn the first coord in this silo
        {
            return siloID * Constants.numLocalCoordPerSilo;
        }

        public static int MapCoordIDToNeighborID(int coordID)
        {
            var siloID = MapCoordIDToSiloID(coordID);
            return (coordID + 1) % Constants.numLocalCoordPerSilo + siloID * Constants.numLocalCoordPerSilo;
        }
    }
}