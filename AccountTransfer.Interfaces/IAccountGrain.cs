using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface;
using Utilities;
using System;

namespace AccountTransfer.Interfaces
{
    public class TransferInput
    {
        public UInt32 sourceAccount;
        public UInt32 destinationAccount;
        public float transferAmount;

        public TransferInput(uint sourceAccount, uint destinationAccount, float transferAmount)
        {
            this.sourceAccount = sourceAccount;
            this.destinationAccount = destinationAccount;
            this.transferAmount = transferAmount;
        }
    }

    public class TransferOneToMultiInput
    {
        public UInt32 sourceAccount;
        public List<UInt32> destinationAccounts;
        public float transferAmount;

        public TransferOneToMultiInput(uint sourceAccount, List<uint> destinationAccounts, float transferAmount)
        {
            this.sourceAccount = sourceAccount;
            this.destinationAccounts = destinationAccounts;
            this.transferAmount = transferAmount;
        }
    }

    public interface IAccountGrain : ITransactionExecutionGrain
    {
        Task<FunctionResult> GetBalance(FunctionInput fin);
        Task<FunctionResult> Withdraw(FunctionInput fin);
        Task<FunctionResult> Deposit(FunctionInput fin);
        Task<FunctionResult> Transfer(FunctionInput fin);
        Task<FunctionResult> TransferOneToMulti(FunctionInput fin);

        Task<int> ActivateGrain();
    }
}
