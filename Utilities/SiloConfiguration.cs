namespace Utilities
{
    public struct SiloConfiguration
    {
        public readonly int numCPUPerSilo;
        public readonly ImplementationType implementationType;
        public readonly BenchmarkType benchmarkType;
        public readonly bool loggingEnabled;

        public readonly int NUM_OrderGrain_PER_D;

        public SiloConfiguration(int numCPUPerSilo, ImplementationType implementationType, BenchmarkType benchmarkType, bool loggingEnabled, int NUM_OrderGrain_PER_D)
        {
            this.numCPUPerSilo = numCPUPerSilo;
            this.implementationType = implementationType;
            this.benchmarkType = benchmarkType;
            this.loggingEnabled = loggingEnabled;
            this.NUM_OrderGrain_PER_D = NUM_OrderGrain_PER_D;
        }
    }
}