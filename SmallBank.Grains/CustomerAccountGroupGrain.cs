using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Implementation;
using SmallBank.Interfaces;
using Utilities;

namespace SmallBank.Grains
{
    using AmalgamateInput = Tuple<UInt32, UInt32>;
    using WriteCheckInput = Tuple<String, float>;
    using TransactSavingInput = Tuple<String, float>;
    using DepositCheckingInput = Tuple<Tuple<String, UInt32>, float>;
    using BalanceInput = String;
    //Source AccountID, Destination AccountID, Destination Grain ID, Amount
    using TransferInput = Tuple<UInt32, UInt32, UInt32, float>;
    //Source AccountID, Amount, List<Tuple<Destination Account ID, Destination Account Grain ID, Amount>>
    using MultiTransferInput = Tuple<UInt32, float, List<Tuple<UInt32, UInt32, float>>>; 

    [Serializable]
    public class CustomerAccountGroup : ICloneable
    {
        public Dictionary<String, UInt32> account;
        public Dictionary<UInt32, float> savingAccount;
        public Dictionary<UInt32, float> checkingAccount;
        object ICloneable.Clone()
        {
            var clonedCustomerAccount = new CustomerAccountGroup();
            clonedCustomerAccount.account = new Dictionary<string, UInt32>(account);
            clonedCustomerAccount.savingAccount = new Dictionary<UInt32, float>(savingAccount);
            clonedCustomerAccount.checkingAccount = new Dictionary<UInt32, float>(checkingAccount);
            return clonedCustomerAccount;
        }
    }


    class CustomerAccountGroupGrain : TransactionExecutionGrain<CustomerAccountGroup>, ICustomerAccountGroupGrain
    {
        private UInt32 MapCustomerIdToGroup(UInt32 id)
        {
            return id; //You can can also range/hash partition
        }
        public CustomerAccountGroupGrain() : base("SmallBank.Grains.CustomerAccountGroupGrain")
        {
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.Amalgamate(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.Read(context);
                var tuple = (AmalgamateInput)fin.inputObject;
                var id = tuple.Item1;
                float balance = 0;
                
                if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                {
                    ret.setException();
                }
                else
                {                    
                    balance = myState.savingAccount[id] + myState.checkingAccount[id];                    
                }

                //By invoking with 0 amount and no state mutation, we make the execution deterministic
                var destGrain = this.GrainFactory.GetGrain<ICustomerAccountGroupGrain>(MapCustomerIdToGroup(tuple.Item2));
                var result = await destGrain.Execute(new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositChecking", new FunctionInput(fin, new Tuple<Tuple<String, UInt32>, float>(new Tuple<String, UInt32>(String.Empty, id), balance))));
                ret.mergeWithFunctionResult(result);
                if(!ret.hasException())
                {
                    //By ensuring state mutation on no exception, we make it deterministic
                    myState.savingAccount[id] = 0;
                    myState.checkingAccount[id] = 0;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.Balance(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult(-1);
            try
            {
                var myState = await state.Read(context);
                var custName = (BalanceInput)fin.inputObject;
                if (myState.account.ContainsKey(custName))
                {
                    var id = myState.account[custName];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        ret.setException();
                        return ret;
                    }
                    ret.setResult(myState.savingAccount[id] + myState.checkingAccount[id]);
                }
                else
                {
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.DepositChecking(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (DepositCheckingInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!String.IsNullOrEmpty(custName))
                {
                    id = myState.account[custName];
                }
                else
                {
                    ret.setException();
                    return ret;
                }
                if (!myState.checkingAccount.ContainsKey(id))
                {
                    ret.setException();
                    return ret;
                }
                myState.checkingAccount[id] += inputTuple.Item2; //Can also be negative for checking account                
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.MultiTransfer(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (MultiTransferInput)fin.inputObject;
                if (myState.savingAccount[inputTuple.Item1] < inputTuple.Item2)
                {
                    ret.setException();
                    return ret;
                }
                else
                {
                    List<Tuple<UInt32, UInt32, float>> destinations = inputTuple.Item3;
                    List<Task<FunctionResult>> tasks = new List<Task<FunctionResult>>();
                    foreach (var tuple in destinations){
                        var destination = this.GrainFactory.GetGrain<ICustomerAccountGroupGrain>(Helper.convertUInt32ToGuid(tuple.Item2));
                        FunctionInput funcInput = new FunctionInput(fin, new DepositSavingInput(tuple.Item1, tuple.Item3));
                        FunctionCall funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositSaving", funcInput);
                        tasks.Add(destination.Execute(funcCall));
                    }
                    await Task.WhenAll(tasks);
                    myState.savingAccount[inputTuple.Item1] -= inputTuple.Item2;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.TransactSaving(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (TransactSavingInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id))
                    {
                        ret.setException();
                        return ret;
                    }
                    if (myState.savingAccount[id] < inputTuple.Item2)
                    {
                        ret.setException();
                        return ret;
                    }
                    myState.savingAccount[id] -= inputTuple.Item2;
                }
                else
                {
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.Transfer(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (TransferInput)fin.inputObject;
                if (myState.savingAccount[inputTuple.Item1] < inputTuple.Item4)
                {
                    ret.setException();
                    return ret;
                }
                else
                {
                    var destination = this.GrainFactory.GetGrain<ICustomerAccountGroupGrain>(Helper.convertUInt32ToGuid(inputTuple.Item3));
                    FunctionInput funcInput = new FunctionInput(fin, new DepositSavingInput(inputTuple.Item2, inputTuple.Item4));
                    FunctionCall funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositSaving", funcInput);
                    Task<FunctionResult> task = destination.Execute(funcCall);
                    await task;
                    if (task.Result.hasException() == true)
                    {
                        ret.setException();
                        return ret;
                    }
                    myState.savingAccount[inputTuple.Item1] -= inputTuple.Item4;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.WriteCheck(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (WriteCheckInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        ret.setException();
                        return ret;
                    }
                    if (myState.savingAccount[id] + myState.checkingAccount[id] < inputTuple.Item2)
                    {
                        myState.checkingAccount[id] -= (inputTuple.Item2 + 1); //Pay a penalty                        
                    }
                    else
                    {
                        myState.checkingAccount[id] -= inputTuple.Item2;
                    }
                }
                else
                {
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }
    }
}
