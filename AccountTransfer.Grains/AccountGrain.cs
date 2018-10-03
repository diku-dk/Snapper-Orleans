using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.CodeGeneration;
using Orleans.Transactions.Abstractions;
using AccountTransfer.Interfaces;
using System.Collections.Generic;
using Concurrency.Implementation;
using Concurrency.Utilities;
using Concurrency.Interface.Nondeterministic;
using Concurrency.Implementation.Nondeterministic;
using Concurrency.Implementation.Deterministic;

namespace AccountTransfer.Grains
{
    [Serializable]
    public class Balance : ICloneable

    {
        public int value = 1000;
        public int count = 0;
        public Balance(Balance balance)
        {
            this.value = balance.value;
            this.count = balance.count;
        }
        public Balance()
        {
            this.value = 1000;
            this.count = 0;
        }
        object ICloneable.Clone()
        {
            return new Balance(this);
        }
    }

    public class AccountGrain : TransactionExecutionGrain<Balance>, IAccountGrain
    {
        public AccountGrain()
        {
            int type = 0;
            this.myUserClassName = "AccountTransfer.Grains.AccountGrain";
            Balance balance = new Balance();
            if (type == 0)
            {
                state = new TimestampTransactionalState<Balance>(balance);

            }
            else if (type == 1)
            {
                state = new S2PLTransactionalState<Balance>(balance);
            }
            else if (type == 2)
            {
                state = new DeterministicTransactionalState<Balance>(balance);
            }      
        }

        public async Task<FunctionResult> Deposit(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            List<object> inputs = fin.inputObjects;
            FunctionResult ret = new FunctionResult();
            try
            {
                Balance balance = await state.ReadWrite(context.transactionID);
                int amount = (int)inputs[0];
                balance.value += amount;
                //Console.WriteLine($"\n\n After deposit of Tx: {context.transactionID}, {this.myPrimaryKey} balance: {balance.value}.\n\n");
            }
            catch(Exception)
            {
                ret.setException(true);
            }
            
            return ret;
        }

        public async Task<FunctionResult> Withdraw(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            List<object> inputs = fin.inputObjects;
            FunctionResult ret = new FunctionResult();
            try
            {
                Balance balance = await state.ReadWrite(context.transactionID);
                int amount = (int)inputs[0];
                balance.value -= amount;
                //Console.WriteLine($"\n\n After withdraw of Tx: {context.transactionID}, {this.myPrimaryKey} balance: {balance.value}.\n\n");
            }
            catch (Exception)
            {
                ret.setException(true);
            }
            return ret;
        }

        public async Task<FunctionResult> GetBalance(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            List<object> inputs = fin.inputObjects;
            FunctionResult ret = new FunctionResult();
            int v = -1;
            try
            {
                Balance balance = await state.ReadWrite(context.transactionID);
                v = balance.value;
            }
            catch (Exception)
            {
                ret.setException(true);
            }
            ret.setResult(v);
            return ret;
        }
    }
}

