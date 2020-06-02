using System;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Placement;
using Orleans.Runtime;
using Orleans.Runtime.Placement;

namespace Concurrency.Implementation
{
    public class CoordPlacement : IPlacementDirector
    {
        public Task<SiloAddress> OnAddActivation(PlacementStrategy strategy, PlacementTarget target, IPlacementContext context)
        {
            var silos = context.GetCompatibleSilos(target).OrderBy(s => s).ToArray();
            var silo = GetSiloNumber(target.GrainIdentity.PrimaryKey, silos.Length);
            return Task.FromResult(silos[silo]);
        }

        private int GetSiloNumber(Guid coordID, int siloLen)
        {
            var IDs = coordID.ToString().Split("-", StringSplitOptions.RemoveEmptyEntries);
            var number = Convert.ToInt32(IDs[0]);
            Console.WriteLine($"Convert guid {coordID} to int {number}.");
            return number % siloLen;
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
