﻿using Orleans.Concurrency;
using System.Threading.Tasks;
using Orleans.Transactions;
using Utilities;

namespace TPCC.Interfaces
{
    public interface IEventualCustomerGrain : Orleans.IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        Task<TransactionResult> StartTransaction(string startFunc, object funcInput);
    }
}