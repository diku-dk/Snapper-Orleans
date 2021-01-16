using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace ExperimentProcess
{
    public interface IBenchmark
    {
        void generateBenchmark(WorkloadConfiguration workloadConfig);
        Task<TransactionResult> newTransaction(IClusterClient client, int tid);
    }
}
