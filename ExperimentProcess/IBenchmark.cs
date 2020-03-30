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
        Task<FunctionResult> newTransaction(IClusterClient client);
    }
}
