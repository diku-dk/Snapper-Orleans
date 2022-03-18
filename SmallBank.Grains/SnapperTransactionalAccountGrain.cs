using System;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface.Logging;

namespace SmallBank.Grains
{
    using MultiTransferInput = Tuple<float, List<int>>;  // money, List<to account>

    public class SnapperTransactionalAccountGrain : TransactionExecutionGrain<BankAccount>, ISnapperTransactionalAccountGrain
    {
        public SnapperTransactionalAccountGrain(ILoggerGroup loggerGroup) : base(loggerGroup, "SmallBank.Grains.BankAccountGrain")
        {
        }

        public async Task<TransactionResult> Init(TransactionContext context, object funcInput)
        {
            var accountID = (int)funcInput;
            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.accountID = accountID;
            myState.balance = int.MaxValue;
            return new TransactionResult();
        }

        public async Task<TransactionResult> MultiTransfer(TransactionContext context, object funcInput)
        {
            var input = (MultiTransferInput)funcInput;
            var money = input.Item1;
            var toAccounts = input.Item2;
            var myState = await GetState(context, AccessMode.ReadWrite);

            myState.balance -= money * toAccounts.Count;

            var task = new List<Task>();
            foreach (var accountID in toAccounts)
            {
                if (accountID != myState.accountID)
                {
                    var funcCall = new FunctionCall("Deposit", money, typeof(SnapperTransactionalAccountGrain));
                    var t = CallGrain(context, accountID, "SmallBank.Grains.BankAccountGrain", funcCall);
                    task.Add(t);
                } 
                else task.Add(Deposit(context, money));
            }
            await Task.WhenAll(task);
            return new TransactionResult();
        }
       
        public async Task<TransactionResult> Deposit(TransactionContext context, object funcInput)
        {
            var money = (float)funcInput;
            var myState = await GetState(context, AccessMode.ReadWrite);
            myState.balance += money;
            return new TransactionResult();
        }

        public async Task<TransactionResult> Balance(TransactionContext context, object funcInput)
        {
            var myState = await GetState(context, AccessMode.Read);
            return new TransactionResult(myState.balance);
        }
    }
}