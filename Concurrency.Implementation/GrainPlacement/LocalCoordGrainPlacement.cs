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
                // in the simple architecture, all local coordinators are put in a separate silo
                if (Constants.hierarchicalCoord == false) silo = Constants.numSilo;
                else
                {
                    var coordID = (int)target.GrainIdentity.PrimaryKeyLong;
                    silo = LocalCoordGrainPlacementHelper.MapCoordIDToSiloID(coordID);
                }
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
}