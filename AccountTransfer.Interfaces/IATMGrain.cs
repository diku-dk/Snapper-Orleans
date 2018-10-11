using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface;
using Concurrency.Utilities;
using Orleans;
using Orleans.Concurrency;

namespace AccountTransfer.Interfaces
{
    public class TransferInput {
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

    
    public interface IATMGrain : ITransactionExecutionGrain
    {
        Task<String> getPromise();
        Task setpromise();

        [AlwaysInterleave]
        Task<int> testReentrance(int i);

        [AlwaysInterleave]
        Task setpromise(int i);

        Task<FunctionResult> Transfer(FunctionInput input);

        Task<FunctionResult> TransferOneToMulti(FunctionInput input);

        Task<int> ActivateGrain();
    }

}
