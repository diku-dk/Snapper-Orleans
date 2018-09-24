using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.CodeGeneration;
using Orleans.Transactions.Abstractions;
using AccountTransfer.Interfaces;
using System.Collections.Generic;
using Concurrency.Implementation;

namespace AccountTransfer.Grains
{

    public class AccountGrain : TransactionExecutionGrain, IAccountGrain
    {
        //private readonly ITransactionalState<Balance> balance;
        public int balance = 1000;
        public   Task<List<object>> Deposit(List<object> inputs)
        {
            int amount = (int)inputs[0];
            this.balance += amount;
            List<object> ret = new List<object>();
            return Task.FromResult(ret);
        }

        public  Task<List<object>> Withdraw(List<object> inputs)
        {
            int amount = (int)inputs[0];
            this.balance -= amount;
            List<object> ret = new List<object>();
            return Task.FromResult(ret);
        }

        public Task<int> GetBalance()
        {
            return Task.FromResult(balance);
        }
    }
}

