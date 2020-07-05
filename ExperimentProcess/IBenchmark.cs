using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace ExperimentProcess
{
    public interface IBenchmark
    {
        void generateBenchmark(WorkloadConfiguration workloadConfig);
        Task<FunctionResult> newTransaction(IClusterClient client, int global_tid);    // changed by Yijian
        Task<FunctionResult> newTransaction(IClusterClient client, int global_tid, TxnType type);    // added by Yijian
        //Task<TransactionContext> newTransaction(IClusterClient client, int global_tid);
    }
}
