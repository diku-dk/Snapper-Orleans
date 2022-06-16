using System;
using Utilities;
using Persist.Interfaces;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Implementation;
using Orleans.Transactions;
using Utilities;

namespace SmallBank.Grains
{
    using InitAccountInput = Tuple<int, int>;

    using DepositInput = Tuple<Tuple<string, int>, float>;
    using MultiTransferInput = Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>;  //Source AccountID, Amount, List<Tuple<Account Name, Account ID, Grain ID>>
    
    using NewDepositInput = Tuple<Tuple<string, int>, float, bool>;
    using NewMultiTransferInput = Tuple<Tuple<string, int>, float, List<Tuple<string, int>>, int>;

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

        public async Task<TransactionResult> Init(MyTransactionContext context, object funcInput)
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

        public async Task<TransactionResult> GetBalance(MyTransactionContext context, object funcInput)
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

        public async Task<TransactionResult> MultiTransfer(MyTransactionContext context, object funcInput)
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
                    var tasks = new List<Task>();
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2);
                        if (gID == myState.GroupID) tasks.Add(Deposit(context, input));
                        else
                        {
                            var funcCall = new FunctionCall("Deposit", input, typeof(CustomerAccountGroupGrain));
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
        }

        public async Task<TransactionResult> Deposit(MyTransactionContext context, object funcInput)
        {
            TransactionResult res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
                var inputTuple = (DepositInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
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

        public async Task<TransactionResult> MultiTransferNoDeadlock(MyTransactionContext context, object funcInput)
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
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2);
                        if (gID == myState.GroupID)
                        {
                            var task = Deposit(context, input);
                            await task;
                        }
                        else
                        {
                            var funcCall = new FunctionCall("Deposit", input, typeof(CustomerAccountGroupGrain));
                            var task = CallGrain(context, gID, "SmallBank.Grains.CustomerAccountGroupGrain", funcCall);
                            await task;
                        }
                    }
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        public async Task<TransactionResult> MultiTransferWithNOOP(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            res.beforeExeTime = DateTime.Now;
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
                res.beforeUpdate1Time = DateTime.Now;

                var inputTuple = (NewMultiTransferInput)funcInput;   // <Source AccountID>, Amount, List<Dest AccountID>, num writer grains
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
                    var count = 0;
                    var write = true;
                    res.callGrainTime = DateTime.Now;
                    var numWriter = inputTuple.Item4;
                    foreach (var tuple in destinations)
                    {
                        count++;
                        if (count == numWriter) write = false;
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new NewDepositInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2, write);
                        if (gID == myState.GroupID)
                        {
                            var task = DepositWithNOOP(context, input);
                            await task;
                        }
                        else
                        {
                            var funcCall = new FunctionCall("DepositWithNOOP", input, typeof(CustomerAccountGroupGrain));
                            var task = CallGrain(context, gID, "SmallBank.Grains.CustomerAccountGroupGrain", funcCall);
                            await task;
                        }
                    }
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            res.afterExeTime = DateTime.Now;
            return res;
        }

        public async Task<TransactionResult> DepositWithNOOP(MyTransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {

                var inputTuple = (NewDepositInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                var write = inputTuple.Item3;
                if (write == false) return res;
                
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
    }
}