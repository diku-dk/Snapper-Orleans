using System;
using Utilities;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using System.Diagnostics;

namespace Concurrency.Implementation.GrainPlacement
{
    public class GlobalCoordGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            var silo = 0;
            if (Constants.multiSilo)
            {
                Debug.Assert(silos.Length == Constants.numSilo + 1);
                silo = Constants.numSilo;      // put all global coords in last silo (the list of silos cannot change)
            }
            return Task.FromResult(silos[silo]);
        }
    }

    [Serializable]
    public class GlobalCoordGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class GlobalCoordGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public GlobalCoordGrainPlacementStrategyAttribute() : base(new GlobalCoordGrainPlacementStrategy())
        {
        }
    }

    public static class GlobalCoordGrainPlacementHelper
    {
        public static int MapCoordIDToNeighborID(int coordID)
        {
            return (coordID + 1) % Constants.numLocalCoordPerSilo;
        }
    }
}