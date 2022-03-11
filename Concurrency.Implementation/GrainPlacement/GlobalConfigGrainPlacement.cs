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
    public class GlobalConfigGrainPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var coordID = (int)target.GrainIdentity.PrimaryKeyLong;
            Debug.Assert(coordID == 0);     // there is only one global config grain in the whole system
            var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();   // get the list of registered silo hosts
            var silo = 0;
            if (Constants.multiSilo) silo = Constants.numSilo;  // put global config grain in last silo
            return Task.FromResult(silos[silo]);
        }
    }

    [Serializable]
    public class GlobalConfigGrainPlacementStrategy : PlacementStrategy
    {
    }

    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public sealed class GlobalConfigGrainPlacementStrategyAttribute : PlacementAttribute
    {
        public GlobalConfigGrainPlacementStrategyAttribute() : base(new GlobalConfigGrainPlacementStrategy())
        {
        }
    }
}