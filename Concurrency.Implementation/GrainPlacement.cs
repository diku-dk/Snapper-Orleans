using System;
using Utilities;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation
{
    public class GrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            var silo = 0;
            if (Constants.multiSilo)
            {
                var grainID = (int)target.GrainIdentity.PrimaryKeyLong;
                silo = grainID / Constants.numGrainPerSilo;
            }
            return Task.FromResult(silos[silo]);
        }
    }

    [Serializable]
    public class GrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class GrainPlacementStrategyAttribute : PlacementAttribute
    {
        public GrainPlacementStrategyAttribute() :
            base(new GrainPlacementStrategy())
        {
        }
    }
}
