using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace ExperimentProcess
{
    public interface IBenchmark
    {
        void generateBenchmark(WorkloadConfiguration workloadConfig, double skew, double hotGrainRatio);
        Task<TransactionResult> newTransaction(IClusterClient client, int eIndex);
        void generateGrainIDs(int threadIndex, int epoch);
        void setIndex(int num);
    }
}