using Orleans;
using Utilities;
using System.Threading.Tasks;

namespace ExperimentProcess
{
    public interface IBenchmark
    {
        void GenerateBenchmark(WorkloadConfiguration workloadConfig, bool isDet);
        Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data);
    }
}
