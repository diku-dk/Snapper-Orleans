using System;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Concurrency;

namespace SmallBank.Grains
{
    using MultiTransferInput = Tuple<float, List<int>>;  // money, List<to account>

    [Reentrant]
    class NonTransactionalAccountGrain : Orleans.Grain, INonTransactionalAccountGrain
    {
        BankAccount state = new BankAccount();

        async Task<TransactionResult> Init(object funcInput)
        {
            await Task.CompletedTask;
            var accountID = (int)funcInput;
            state.accountID = accountID;
            state.balance = int.MaxValue;
            return new TransactionResult();
        }

        async Task<TransactionResult> Balance(object _)
        {
            await Task.CompletedTask;
            return new TransactionResult(state.balance);
        }

        async Task<TransactionResult> Deposit(object funcInput)
        {
            await Task.CompletedTask;
            var money = (float)funcInput;
            state.balance += money;
            return new TransactionResult();
        }

        async Task<TransactionResult> MultiTransfer(object funcInput)
        {
            var input = (MultiTransferInput)funcInput;
            var money = input.Item1;
            var toAccounts = input.Item2;

            state.balance -= money * toAccounts.Count;

            var task = new List<Task>();
            foreach (var accountID in toAccounts)
            {
                if (accountID != state.accountID)
                {
                    var grain = GrainFactory.GetGrain<INonTransactionalAccountGrain>(accountID);
                    var t = grain.StartTransaction("Deposit", money);
                    task.Add(t);
                }
                else task.Add(Deposit(money));
            }
            await Task.WhenAll(task);
            return new TransactionResult();
        }

        public Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            TxnType fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case TxnType.Init:
                    return Init(funcInput);
                case TxnType.Balance:
                    return Balance(funcInput);
                case TxnType.MultiTransfer:
                    return MultiTransfer(funcInput);
                case TxnType.Deposit:
                    return Deposit(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}