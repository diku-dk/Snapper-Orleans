using System;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Implementation;

namespace SmallBank.Grains
{
    using WriteCheckInput = Tuple<string, float>;
    using TransactSavingInput = Tuple<string, float>;
    using DepositCheckingInput = Tuple<Tuple<string, int>, float>;
    using BalanceInput = String;
    //Source AccountID, Destination AccountID, Destination Grain ID, Amount
    using TransferInput = Tuple<Tuple<string, int>, Tuple<string, int>, float>;
    //Source AccountID, Amount, List<Tuple<Account Name, Account ID, Grain ID>>
    using MultiTransferInput = Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>;
    using InitAccountInput = Tuple<int, int>;

    [Serializable]
    public class CustomerAccountGroup : ICloneable
    {
        public Dictionary<string, int> account;
        public Dictionary<int, float> savingAccount;
        public Dictionary<int, float> checkingAccount;
        public int GroupID;

        public CustomerAccountGroup()
        {
            account = new Dictionary<string, int>();
            savingAccount = new Dictionary<int, float>();
            checkingAccount = new Dictionary<int, float>();
        }

        object ICloneable.Clone()
        {
            var clonedCustomerAccount = new CustomerAccountGroup();
            clonedCustomerAccount.account = new Dictionary<string, int>(account);
            clonedCustomerAccount.savingAccount = new Dictionary<int, float>(savingAccount);
            clonedCustomerAccount.checkingAccount = new Dictionary<int, float>(checkingAccount);
            clonedCustomerAccount.GroupID = GroupID;
            return clonedCustomerAccount;
        }
    }

    public class CustomerAccountGroupGrain : TransactionExecutionGrain<CustomerAccountGroup>, ICustomerAccountGroupGrain
    {
        public int numAccountPerGroup = 1;

        private int MapCustomerIdToGroup(int accountID)
        {
            return accountID / numAccountPerGroup; //You can can also range/hash partition
        }


        public CustomerAccountGroupGrain() : base("SmallBank.Grains.CustomerAccountGroupGrain")
        {
        }

        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var tuple = (InitAccountInput)fin.inputObject;
                numAccountPerGroup = tuple.Item1;
                myState.GroupID = tuple.Item2;

                int minAccountID = myState.GroupID * numAccountPerGroup;
                for (int i = 0; i < numAccountPerGroup; i++)
                {
                    int accountId = minAccountID + i;
                    myState.account.Add(accountId.ToString(), accountId);
                    myState.savingAccount.Add(accountId, int.MaxValue);
                    myState.checkingAccount.Add(accountId, int.MaxValue);
                }
            }
            catch (Exception)
            {
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

        public async Task<FunctionResult> DepositChecking(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (DepositCheckingInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id))
                {
                    ret.setException();
                    return ret;
                }
                myState.checkingAccount[id] += inputTuple.Item2;
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> MultiTransfer(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (MultiTransferInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item2 * inputTuple.Item3.Count)
                {
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
                        var funcInput = new FunctionInput(fin, new DepositCheckingInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2));
                        if (gID == myState.GroupID)
                        {
                            Task<FunctionResult> localCall = DepositChecking(funcInput);
                            tasks.Add(localCall);
                        }
                        else
                        {
                            var destination = GrainFactory.GetGrain<ICustomerAccountGroupGrain>(gID);
                            var funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositChecking", funcInput);
                            tasks.Add(destination.Execute(funcCall));
                        }
                    }
                    if (!context.isDeterministic)
                    {
                        await Task.WhenAll(tasks);
                        foreach (Task<FunctionResult> task in tasks) ret.mergeWithFunctionResult(task.Result);
                    }
                }
            }
            catch (Exception)
            {
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
                var inputTuple = (TransactSavingInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id) || myState.savingAccount[id] < inputTuple.Item2)
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

        public async Task<FunctionResult> Transfer(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (TransferInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!BalanceInput.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item3)
                {
                    ret.setException();
                    return ret;
                }
                myState.checkingAccount[id] -= inputTuple.Item3;

                var gID = MapCustomerIdToGroup(inputTuple.Item2.Item2);
                var funcInput = new FunctionInput(fin, new DepositCheckingInput(inputTuple.Item2, inputTuple.Item3));
                Task<FunctionResult> task;
                if (gID == myState.GroupID) task = DepositChecking(funcInput);
                else
                {
                    var destination = GrainFactory.GetGrain<ICustomerAccountGroupGrain>(gID);
                    var funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositChecking", funcInput);
                    task = destination.Execute(funcCall);
                }

                if (!context.isDeterministic)
                {
                    await task;
                    ret.mergeWithFunctionResult(task.Result);
                }
            }
            catch (Exception)
            {
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
                        myState.checkingAccount[id] -= (inputTuple.Item2 + 1);    // Pay a penalty  
                    else myState.checkingAccount[id] -= inputTuple.Item2;
                    myState.checkingAccount[id] -= inputTuple.Item2;
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
