using System;
using Utilities;
using Orleans.Concurrency;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Transactions.Abstractions;
using Concurrency.Interface.Logging;

namespace SmallBank.Grains
{
    using MultiTransferInput = Tuple<float, List<int>>;  // money, List<to account>

    [Reentrant]
    class OrleansTransactionalAccountGrain : Orleans.Grain, IOrleansTransactionalAccountGrain
    {
        readonly ILoggerGroup loggerGroup;
        readonly ITransactionalState<BankAccount> state;

        public OrleansTransactionalAccountGrain(ILoggerGroup loggerGroup, [TransactionalState("state")] ITransactionalState<BankAccount> state)
        {
            this.loggerGroup = loggerGroup;
            this.state = state ?? throw new ArgumentNullException(nameof(state));
        }

        public Task SetIOCount()
        {
            loggerGroup.SetIOCount();
            return Task.CompletedTask;
        }

        public Task<long> GetIOCount()
        {
            return Task.FromResult(loggerGroup.GetIOCount());
        }

        async Task<TransactionResult> Init(object funcInput)
        {
            var accountID = (int)funcInput;
            await state.PerformUpdate(s =>
            {
                s.accountID = accountID;
                s.balance = int.MaxValue;
            });
            return new TransactionResult();
        }

        async Task<TransactionResult> Balance(object _)
        {
            var myState = await state.PerformRead(s => s);
            return new TransactionResult(myState.balance);
        }

        async Task<TransactionResult> MultiTransfer(object funcInput)
        {
            var input = (MultiTransferInput)funcInput;
            var money = input.Item1;
            var toAccounts = input.Item2;
            var myAccountID = await state.PerformUpdate(s =>
            {
                s.balance -= money * toAccounts.Count;
                return s.accountID;
            });

            var task = new List<Task>();
            foreach (var accountID in toAccounts)
            {
                if (accountID != myAccountID)
                {
                    var grain = GrainFactory.GetGrain<IOrleansTransactionalAccountGrain>(accountID);
                    var t = grain.StartTransaction("Deposit", money);
                    task.Add(t);
                }
                else task.Add(Deposit(money));
            }
            await Task.WhenAll(task);
            return new TransactionResult();
        }
        
        private async Task<TransactionResult> Deposit(object funcInput)
        {
            var money = (float)funcInput;
            await state.PerformUpdate(s => s.balance += money);
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
                case TxnType.Deposit:
                    return Deposit(funcInput);
                case TxnType.MultiTransfer:
                    return MultiTransfer(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}