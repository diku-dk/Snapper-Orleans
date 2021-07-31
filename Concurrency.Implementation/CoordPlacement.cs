using System;
using Utilities;
using System.Linq;
using Orleans.Runtime;
using Orleans.Placement;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using System.Diagnostics;

namespace Concurrency.Implementation
{
    public class CoordPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            var silo = 0;
            if (Constants.multiSilo)
            {
                Debug.Assert(silos.Length == Constants.numSilo + 1);
                silo = Constants.numSilo;               // put all coords in last silo
            } 
            return Task.FromResult(silos[silo]);
        }
    }

    [Serializable]
    public class CoordPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class CoordPlacementStrategyAttribute : PlacementAttribute
    {
        public CoordPlacementStrategyAttribute() :
            base(new CoordPlacementStrategy())
        {
        }
    }
}
