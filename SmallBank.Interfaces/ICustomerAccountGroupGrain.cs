using System;
using System.Collections.Generic;
using System.Text;
using Concurrency.Interface;
using System.Threading.Tasks;
using Utilities;

namespace SmallBank.Interfaces
{
    public interface ICustomerAccountGroupGrain : ITransactionExecutionGrain
    {
        Task<FunctionResult> TransactSaving(FunctionInput fin);

        Task<FunctionResult> DepositChecking(FunctionInput fin);

        Task<FunctionResult> WriteCheck(FunctionInput fin);

        Task<FunctionResult> Amalgamate(FunctionInput fin);

        Task<FunctionResult> Balance(FunctionInput fin);

        Task<FunctionResult> Transfer(FunctionInput fin);

        Task<FunctionResult> MultiTransfer(FunctionInput fin);
    }
}
