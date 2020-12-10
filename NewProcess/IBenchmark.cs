using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace NewProcess
{
    public interface IBenchmark
    {
        void generateBenchmark(WorkloadConfiguration workloadConfig, bool isDet);
        Task<TransactionResult> newTransaction(IClusterClient client, List<int> accountGrains);
    }
}
