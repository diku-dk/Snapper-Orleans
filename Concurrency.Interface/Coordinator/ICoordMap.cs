using Orleans;

namespace Concurrency.Interface.Coordinator
{
    public interface ICoordMap
    {
        void Init(IGrainFactory myGrainFactory);
        ILocalCoordGrain GetLocalCoord(int localCoordID);
        IGlobalCoordGrain GetGlobalCoord(int globalCoordID);
    }
}
