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
        public float value;
        public Balance(Balance balance)
        {
            this.value = balance.value;
        }
        public Balance()
        {
            this.value = 100000;
        }
        object ICloneable.Clone()
        {
            return new Balance(this);
        }
    }

    public class AccountGrain : TransactionExecutionGrain<Balance>, IAccountGrain
    {
        public AccountGrain() : base ("AccountTransfer.Grains.AccountGrain")
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
                ret.setException(MyExceptionType.RWConflict);
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
                ret.setException(MyExceptionType.RWConflict);
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
                Balance balance = await state.Read(context);
                v = balance.value;
            }
            catch (Exception e)
            {
                ret.setException(MyExceptionType.RWConflict);
            }
            ret.setResult(v);
            return ret;
        }

        public async Task<FunctionResult> Transfer(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            var input = (TransferInput)fin.inputObject;
            //Invoke the destination deposit
            IAccountGrain toAccount = this.GrainFactory.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(input.destinationAccount));
            FunctionInput input_1 = new FunctionInput(fin, input.transferAmount);

            FunctionCall c = new FunctionCall(typeof(AccountGrain), "Deposit", input_1);

            FunctionResult result = new FunctionResult();
            FunctionResult ret = await Withdraw(input_1);
            
            result.mergeWithFunctionResult(ret);                        
            //Console.WriteLine($"\n\n ATm transfer from : {input.sourceAccount} to {input.destinationAccount}. \n\n");
            ret = await toAccount.Execute(c);
            result.mergeWithFunctionResult(ret);
            return result;
        }

        public async Task<FunctionResult> TransferOneToMulti(FunctionInput functionInput)
        {
            TransactionContext context = functionInput.context;
            var input = (TransferOneToMultiInput)functionInput.inputObject;
            var resultTasks = new List<Task<FunctionResult>>();
            //Fire destination deposits
            foreach (var destinationAccount in input.destinationAccounts)
            {
                resultTasks.Add(this.GrainFactory.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(destinationAccount)).Execute(new FunctionCall(typeof(AccountGrain), "Deposit", new FunctionInput(functionInput, input.transferAmount))));
            }

            var withDrawInput = new FunctionInput(functionInput, input.transferAmount);            
            FunctionResult ret = await Withdraw(withDrawInput);            
            FunctionResult myResult = new FunctionResult();
            myResult.mergeWithFunctionResult(ret);            
            var results = await Task.WhenAll(resultTasks);            
            foreach (var result in results)
            {
                myResult.mergeWithFunctionResult(result);
            }
            return myResult;
        }
        public Task<int> ActivateGrain()
        {
            return Task.FromResult(1);
        }
    }
}

