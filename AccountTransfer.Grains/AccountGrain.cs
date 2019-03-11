using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.CodeGeneration;
using Orleans.Transactions.Abstractions;
using AccountTransfer.Interfaces;
using System.Collections.Generic;
using Concurrency.Implementation;
using Utilities;
using Concurrency.Interface.Nondeterministic;
using Concurrency.Implementation.Nondeterministic;
using Concurrency.Implementation.Deterministic;

namespace AccountTransfer.Grains
{
    [Serializable]
    public class Balance : ICloneable

    {
        public float value = 1000;
        public Balance(Balance balance)
        {
            this.value = balance.value;
        }
        public Balance()
        {
            this.value = 1000;
        }
        object ICloneable.Clone()
        {
            return new Balance(this);
        }
    }

    public class AccountGrain : TransactionExecutionGrain<Balance>, IAccountGrain
    {
        public AccountGrain() : base (new Balance(), "AccountTransfer.Grains.AccountGrain")
        {
            ;
        }

        public async Task<FunctionResult> Deposit(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            Object input = fin.inputObject;
            FunctionResult ret = new FunctionResult();
            try
            {
                Balance balance = await state.ReadWrite(context);
                var amount = (float)input;
                balance.value += amount;
                //Console.WriteLine($"\n\n After deposit of Tx: {context.transactionID}, {this.myPrimaryKey} balance: {balance.value}.\n\n");
            }
            catch(Exception)
            {
                //Console.WriteLine($"\n {e.Message}");
                ret.setException();
            }
            
            return ret;
        }

        public async Task<FunctionResult> Withdraw(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            Object input = fin.inputObject;
            FunctionResult ret = new FunctionResult();
            try
            {
                Balance balance = await state.ReadWrite(context);
                var amount = (float)input;
                balance.value -= amount;
                //Console.WriteLine($"\n\n After withdraw of Tx: {context.transactionID}, {this.myPrimaryKey} balance: {balance.value}.\n\n");
            }
            catch (Exception)
            {
                //Console.WriteLine($"\n {e.Message}");
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> GetBalance(FunctionInput fin)
        {
            TransactionContext context = fin.context;            
            FunctionResult ret = new FunctionResult();
            float v = -1;
            try
            {
                Balance balance = await state.ReadWrite(context);
                v = balance.value;
            }
            catch (Exception)
            {
                ret.setException();
            }
            ret.setResult(v);
            return ret;
        }

        public Task<int> ActivateGrain()
        {
            return Task.FromResult(1);
        }
    }
}

