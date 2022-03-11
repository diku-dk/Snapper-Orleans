using System;
using Utilities;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation.GrainPlacement
{
    public class LocalConfigGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            var silo = 0;
            if (Constants.multiSilo)
            {
                var grainID = (int)target.GrainIdentity.PrimaryKeyLong;
                silo = LocalConfigGrainPlacementHelper.MapGrainIDToSilo(grainID);         
            }
            return Task.FromResult(silos[silo]);
        }
    }

    [Serializable]
    public class LocalConfigGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class LocalConfigGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public LocalConfigGrainPlacementStrategyAttribute() : base(new LocalConfigGrainPlacementStrategy())
        {
        }
    }

    public static class LocalConfigGrainPlacementHelper
    {
        public static int MapGrainIDToSilo(int grainID)
        {
            return grainID;   // one local config grain per silo
        }
    }
}