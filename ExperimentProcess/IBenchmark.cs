using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace ExperimentProcess
{
    interface IBenchmark
    {
        void generateBenchmark(WorkloadConfiguration workloadConfig, IClusterClient client);
        Task<FunctionResult> newTransaction();
    }
}
