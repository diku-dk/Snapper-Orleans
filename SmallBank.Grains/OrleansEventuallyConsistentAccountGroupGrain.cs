using System;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Transactions;
using Utilities;

namespace SmallBank.Grains
{
    using InitAccountInput = Tuple<int, int>;

    using DepositInput = Tuple<Tuple<string, int>, float>;
    using MultiTransferInput = Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>;  //Source AccountID, Amount, List<Tuple<Account Name, Account ID, Grain ID>>

    class OrleansEventuallyConsistentAccountGroupGrain : Orleans.Grain, IOrleansEventuallyConsistentAccountGroupGrain
    {
        CustomerAccountGroup state = new CustomerAccountGroup();
        public int numAccountPerGroup = 1;

        private int MapCustomerIdToGroup(int accountID)
        {
            return accountID / numAccountPerGroup; //You can can also range/hash partition
        }

        async Task<TransactionResult> Init(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
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
                ret.exception = true;
            }
            return ret;
        }

        async Task<TransactionResult> GetBalance(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var custName = (string)funcInput;
                if (myState.account.ContainsKey(custName))
                {
                    var id = myState.account[custName];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        ret.exception = true;
                        return ret;
                    }
                    ret.resultObject = myState.savingAccount[id] + myState.checkingAccount[id];
                }
                else
                {
                    ret.exception = true;
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }

        async Task<TransactionResult> MultiTransfer(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var inputTuple = (MultiTransferInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item2 * inputTuple.Item3.Count)
                {
                    ret.exception = true;
                    return ret;
                }
                else
                {
                    var destinations = inputTuple.Item3;
                    var tasks = new List<Task<TransactionResult>>();
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2);
                        if (gID == myState.GroupID)
                        {
                            var localCall = Deposit(input);
                            tasks.Add(localCall);
                        }
                        else
                        {
                            var destination = this.GrainFactory.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(gID);
                            var task = destination.StartTransaction("Deposit", input);
                            tasks.Add(task);
                        }
                    }
                    await Task.WhenAll(tasks);
                    myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                }
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }

        async Task<TransactionResult> Deposit(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var inputTuple = (DepositInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id))
                {
                    ret.exception = true;
                    return ret;
                }
                myState.checkingAccount[id] += inputTuple.Item2; //Can also be negative for checking account                
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }

        Task<TransactionResult> IOrleansEventuallyConsistentAccountGroupGrain.StartTransaction(string startFunc, object funcInput)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(funcInput);
                case AllTxnTypes.GetBalance:
                    return GetBalance(funcInput);
                case AllTxnTypes.MultiTransfer:
                    return MultiTransfer(funcInput);
                case AllTxnTypes.Deposit:
                    return Deposit(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}