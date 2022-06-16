using Orleans;
using Utilities;
using System.Threading.Tasks;
using Orleans.Transactions;

namespace SnapperExperimentProcess
{
    public interface IBenchmark
    {
        void GenerateBenchmark(ImplementationType implementationType, bool isDet, bool noDeadlock = false);
        Task<TransactionResult> NewTransaction(IClusterClient client, RequestData data);

        Task<TransactionResult> NewTransactionWithNOOP(IClusterClient client, RequestData data, int numWriter);
    }
}
