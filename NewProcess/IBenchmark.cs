using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace NewProcess
{
    public interface IBenchmark
    {
        void generateBenchmark(WorkloadConfiguration workloadConfig, bool isDet);
        Task<TransactionResult> newTransaction(IClusterClient client, RequestData data);
    }
}
