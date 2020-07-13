using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Concurrency.Implementation;
using Concurrency.Interface;
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
    using TransferInput = Tuple<Tuple<String, UInt32>, Tuple<String, UInt32>, float>;
    //Source AccountID, Amount, List<Tuple<Account Name, Account ID, Grain ID>>
    using MultiTransferInput = Tuple<Tuple<String, UInt32>, float, List<Tuple<String, UInt32>>>;
    using InitAccountInput = Tuple<UInt32, UInt32>;

    [Serializable]
    public class CustomerAccountGroup : ICloneable
    {
        public Dictionary<String, UInt32> account;
        public Dictionary<UInt32, float> savingAccount;
        public Dictionary<UInt32, float> checkingAccount;
        public uint GroupID;

        public CustomerAccountGroup()
        {
            account = new Dictionary<string, UInt32>();
            savingAccount = new Dictionary<UInt32, float>();
            checkingAccount = new Dictionary<UInt32, float>();
        }

        object ICloneable.Clone()
        {
            var clonedCustomerAccount = new CustomerAccountGroup();
            clonedCustomerAccount.account = new Dictionary<string, UInt32>(account);
            clonedCustomerAccount.savingAccount = new Dictionary<UInt32, float>(savingAccount);
            clonedCustomerAccount.checkingAccount = new Dictionary<UInt32, float>(checkingAccount);
            clonedCustomerAccount.GroupID = this.GroupID;
            return clonedCustomerAccount;
        }
    }

    public class CustomerAccountGroupGrain : TransactionExecutionGrain<CustomerAccountGroup>, ICustomerAccountGroupGrain
    {
        public uint numAccountPerGroup = 1;

        private UInt32 MapCustomerIdToGroup(UInt32 accountID)
        {
            return accountID / numAccountPerGroup; //You can can also range/hash partition
        }


        public CustomerAccountGroupGrain() : base("SmallBank.Grains.CustomerAccountGroupGrain")
        {
        }

        public async Task<FunctionResult> InitBankAccounts(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var tuple = (InitAccountInput)fin.inputObject;
                numAccountPerGroup = tuple.Item1;
                myState.GroupID = tuple.Item2;

                uint minAccountID = myState.GroupID * numAccountPerGroup;
                for(uint i=0; i<numAccountPerGroup; i++)
                {
                    uint accountId = minAccountID + i;
                    myState.account.Add(accountId.ToString(), accountId);
                    //myState.savingAccount.Add(accountId, uint.MaxValue);
                    //myState.checkingAccount.Add(accountId, uint.MaxValue);
                    myState.savingAccount.Add(accountId, 1000);
                    myState.checkingAccount.Add(accountId, 1000);
                }
            }
            catch (Exception)
            {
                ret.Exp_RWConflict = true;
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> Balance(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult(-1);
            try
            {
                var myState = await state.Read(context);
                ret.beforeState.Add(myPrimaryKey, myState);
                var custName = (BalanceInput)fin.inputObject;
                if (myState.account.ContainsKey(custName))
                {
                    var id = myState.account[custName];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        ret.Exp_AppLogic = true;
                        ret.setException();
                        return ret;
                    }
                    ret.setResult(myState.savingAccount[id] + myState.checkingAccount[id]);
                }
                else
                {
                    ret.Exp_AppLogic = true;
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.Exp_RWConflict = true;
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> DepositChecking(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                ret.readOnly = false;
                ret.beforeState.Add(myPrimaryKey, myState);
                var inputTuple = (DepositCheckingInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!String.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id))
                {
                    ret.Exp_AppLogic = true;
                    ret.setException();
                    return ret;
                }
                myState.checkingAccount[id] += inputTuple.Item2;
            }
            catch (Exception e)
            {
                ret.Exp_RWConflict = true;
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> MultiTransfer(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                ret.readOnly = false;
                ret.beforeState.Add(myPrimaryKey, myState);
                var inputTuple = (MultiTransferInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!String.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item2 * inputTuple.Item3.Count)
                {
                    ret.Exp_AppLogic = true;
                    ret.setException();
                    return ret;
                }
                else
                {
                    myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                    var destinations = inputTuple.Item3;
                    var tasks = new List<Task<FunctionResult>>();
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var funcInput = new FunctionInput(fin, new DepositCheckingInput(new Tuple<String, UInt32>(tuple.Item1, tuple.Item2), inputTuple.Item2));
                        if (gID == myState.GroupID)
                        {
                            Task<FunctionResult> localCall = DepositChecking(funcInput);
                            tasks.Add(localCall);
                        }
                        else
                        {
                            var destination = this.GrainFactory.GetGrain<ICustomerAccountGroupGrain>(Helper.convertUInt32ToGuid(gID));
                            var funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositChecking", funcInput);
                            tasks.Add(destination.Execute(funcCall));
                        }
                    }
                    await Task.WhenAll(tasks);
                    foreach (Task<FunctionResult> task in tasks) ret.mergeWithFunctionResult(task.Result);
                    // can do rollback here, because the next txn will not start until this PACT is finished
                    if (context.isDeterministic && ret.hasException())    // rollback
                        myState.checkingAccount[id] += inputTuple.Item2 * inputTuple.Item3.Count;
                }
            }
            catch (Exception)
            {
                ret.Exp_RWConflict = true;
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> TransactSaving(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                ret.readOnly = false;
                ret.beforeState.Add(myPrimaryKey, myState);
                var inputTuple = (TransactSavingInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id))
                    {
                        ret.Exp_AppLogic = true;
                        ret.setException();
                        return ret;
                    }
                    if (myState.savingAccount[id] < inputTuple.Item2)
                    {
                        ret.Exp_AppLogic = true;
                        ret.setException();
                        return ret;
                    }
                    myState.savingAccount[id] -= inputTuple.Item2;
                }
                else
                {
                    ret.Exp_AppLogic = true;
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.Exp_RWConflict = true;
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> Transfer(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                ret.readOnly = false;
                ret.beforeState.Add(myPrimaryKey, myState);
                var inputTuple = (TransferInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!String.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item3)
                {
                    ret.Exp_AppLogic = true;
                    ret.setException();
                    return ret;
                }
                myState.checkingAccount[id] -= inputTuple.Item3;

                var gID = this.MapCustomerIdToGroup(inputTuple.Item2.Item2);
                FunctionInput funcInput = new FunctionInput(fin, new DepositCheckingInput(inputTuple.Item2, inputTuple.Item3));
                Task<FunctionResult> task;
                if (gID == myState.GroupID) task = DepositChecking(funcInput);
                else
                {
                    var destination = this.GrainFactory.GetGrain<ICustomerAccountGroupGrain>(Helper.convertUInt32ToGuid(gID));
                    FunctionCall funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositChecking", funcInput);
                    task = destination.Execute(funcCall);
                }
                await task;
                ret.mergeWithFunctionResult(task.Result);
                if (context.isDeterministic && ret.hasException())   // rollback for PACT
                    myState.checkingAccount[id] += inputTuple.Item3; 
            }
            catch (Exception e)
            {
                ret.Exp_RWConflict = true;
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> WriteCheck(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                ret.readOnly = false;
                ret.beforeState.Add(myPrimaryKey, myState);
                var inputTuple = (WriteCheckInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        ret.Exp_AppLogic = true;
                        ret.setException();
                        return ret;
                    }
                    /*
                    if (myState.savingAccount[id] + myState.checkingAccount[id] < inputTuple.Item2)
                        myState.checkingAccount[id] -= (inputTuple.Item2 + 1);    // Pay a penalty  
                    else myState.checkingAccount[id] -= inputTuple.Item2;*/
                    myState.checkingAccount[id] -= inputTuple.Item2;
                }
                else
                {
                    ret.Exp_AppLogic = true;
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.Exp_RWConflict = true;
                ret.setException();
            }
            return ret;
        }
    }
}
