using System;
using System.Collections.Generic;
using System.Text;
using Utilities;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace SmallBank.Interfaces
{    
    public interface IOrleansEventuallyConsistentAccountGroupGrain : Orleans.IGrainWithIntegerKey, Orleans.IGrainWithGuidKey
    {
        [AlwaysInterleave]
        Task<FunctionResult> StartTransaction(String startFunction, FunctionInput inputs);
    }
}
