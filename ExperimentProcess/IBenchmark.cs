using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace ExperimentProcess
{
    public interface IBenchmark
    {
        void generateBenchmark(WorkloadConfiguration workloadConfig, int tid);
        Task<TransactionResult> newTransaction(IClusterClient client);
    }
}
