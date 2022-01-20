using System;
using Utilities;
using Persist.Interfaces;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Implementation;

namespace SmallBank.Grains
{
    using WriteCheckInput = Tuple<string, float>;
    using TransactSavingInput = Tuple<string, float>;
    using DepositCheckingInput = Tuple<Tuple<string, int>, float, bool>;
    // <Source AccountID>, Amount, List<Dest AccountID>
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

        public CustomerAccountGroupGrain(IPersistSingletonGroup persistSingletonGroup) : base(persistSingletonGroup, "SmallBank.Grains.CustomerAccountGroupGrain")
        {
        }

        public async Task<TransactionResult> Init(TransactionContext context, object funcInput)
        {
            TransactionResult res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
                var tuple = (InitAccountInput)funcInput;
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
                res.exception = true;
            }
            return res;
        }

        /*
        public async Task<TransactionResult> MultiTransfer(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
                var inputTuple = (MultiTransferInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item2 * inputTuple.Item3.Count)
                {
                    res.exception = true;
                    return res;
                }
                else
                {
                    myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                    var destinations = inputTuple.Item3;
                    var tasks = new List<Task<TransactionResult>>();
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositCheckingInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2);
                        if (gID == myState.GroupID)
                        {
                            var localCall = DepositChecking(context, input);
                            tasks.Add(localCall);
                        }
                        else
                        {
                            var funcCall = new FunctionCall("DepositChecking", input, typeof(CustomerAccountGroupGrain));
                            tasks.Add(CallGrain(context, gID, "SmallBank.Grains.CustomerAccountGroupGrain", funcCall));
                        }
                    }
                    await Task.WhenAll(tasks);
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }*/
        /*
        // no deadlock
        public async Task<TransactionResult> MultiTransfer(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
                var inputTuple = (MultiTransferInput)funcInput;   // <Source AccountID>, Amount, List<Dest AccountID>
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item2 * inputTuple.Item3.Count)
                {
                    res.exception = true;
                    return res;
                }
                else
                {
                    myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                    var destinations = inputTuple.Item3;
                    res.callGrainTime = DateTime.Now;
                    var count = 0;
                    var write = true;
                    foreach (var tuple in destinations)
                    {
                        count++;
                        if (count == 4) write = false;
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositCheckingInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2, write);
                        if (gID == myState.GroupID)
                        {
                            var task = DepositChecking(context, input);
                            await task;
                        }
                        else
                        {
                            if (write)
                            {
                                var funcCall = new FunctionCall("DepositChecking", input, typeof(CustomerAccountGroupGrain));
                                var task = CallGrain(context, gID, "SmallBank.Grains.CustomerAccountGroupGrain", funcCall);
                                await task;
                            }
                            else 
                            {
                                var grain = GrainFactory.GetGrain<ICustomerAccountGroupGrain>(gID);
                                await grain.DepositChecking(null, input);
                            }
                        }
                    }
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }*/
        
        public async Task<TransactionResult> MultiTransfer(TransactionContext context, object funcInput)    // read only / no-op
        {
            var res = new TransactionResult();
            try
            {
                //_ = await GetState(context, AccessMode.Read);
                var inputTuple = (MultiTransferInput)funcInput;
                var destinations = inputTuple.Item3;
                res.callGrainTime = DateTime.Now;
                foreach (var tuple in destinations)
                {
                    var gID = MapCustomerIdToGroup(tuple.Item2);
                    var input = new DepositCheckingInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2, false);
                    var funcCall = new FunctionCall("DepositChecking", input, typeof(CustomerAccountGroupGrain));
                    var task = CallGrain(context, gID, "SmallBank.Grains.CustomerAccountGroupGrain", funcCall);
                    await task;
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        public async Task<TransactionResult> DepositChecking(TransactionContext context, object funcInput)
        {
;           var res = new TransactionResult();
            try
            {
                
                var inputTuple = (DepositCheckingInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                var write = inputTuple.Item3;
                if (write == false)
                {
                    //_ = await GetState(context, AccessMode.Read);
                    return res;
                } 

                var myState = await GetState(context, AccessMode.ReadWrite);
                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id))
                {
                    res.exception = true;
                    return res;
                }
                myState.checkingAccount[id] += inputTuple.Item2;
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
        
        public async Task<TransactionResult> TransactSaving(TransactionContext context, object funcInput)
        {
            TransactionResult res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
                var inputTuple = (TransactSavingInput)funcInput;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id) || myState.savingAccount[id] < inputTuple.Item2)
                    {
                        res.exception = true;
                        return res;
                    }
                    myState.savingAccount[id] -= inputTuple.Item2;
                }
                else
                {
                    res.exception = true;
                    return res;
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        public async Task<TransactionResult> WriteCheck(TransactionContext context, object funcInput)
        {
            TransactionResult res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
                var inputTuple = (WriteCheckInput)funcInput;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        res.exception = true;
                        return res;
                    }
                    if (myState.savingAccount[id] + myState.checkingAccount[id] < inputTuple.Item2)
                        myState.checkingAccount[id] -= (inputTuple.Item2 + 1);    // Pay a penalty  
                    else myState.checkingAccount[id] -= inputTuple.Item2;
                    myState.checkingAccount[id] -= inputTuple.Item2;
                }
                else
                {
                    res.exception = true;
                    return res;
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        public async Task<TransactionResult> Balance(TransactionContext context, object funcInput)
        {
            TransactionResult res = new TransactionResult(-1);
            try
            {
                var myState = await GetState(context, AccessMode.Read);
                var custName = (string)funcInput;
                if (myState.account.ContainsKey(custName))
                {
                    var id = myState.account[custName];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        res.exception = true;
                        return res;
                    }
                    res.resultObject = myState.savingAccount[id] + myState.checkingAccount[id];
                }
                else
                {
                    res.exception = true;
                    return res;
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}