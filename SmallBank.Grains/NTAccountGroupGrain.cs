using System;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace SmallBank.Grains
{
    using DepositInput = Tuple<Tuple<string, int>, float>;
    using BalanceInput = String;
    //Source AccountID, Amount, List<Tuple<Account Name, Account ID, Grain ID>>
    using MultiTransferInput = Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>;
    using InitAccountInput = Tuple<int, int>;

    class NTAccountGroupGrain : Orleans.Grain, INTAccountGroupGrain
    {
        CustomerAccountGroup state = new CustomerAccountGroup();
        public int numAccountPerGroup = 1;

        private int MapCustomerIdToGroup(int accountID)
        {
            return accountID / numAccountPerGroup; //You can can also range/hash partition
        }

        private async Task<TransactionResult> Balance(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var custName = (BalanceInput)funcInput;
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

        private async Task<TransactionResult> Deposit(object funcInput)
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

        public async Task<TransactionResult> MultiTransfer(object funcInput)
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
                            var destination = this.GrainFactory.GetGrain<INTAccountGroupGrain>(gID);
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

        public async Task<TransactionResult> Init(object funcInput)
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

        Task<TransactionResult> INTAccountGroupGrain.StartTransaction(string startFunc, object funcInput)
        {
            TxnType fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case TxnType.Init:
                    return Init(funcInput);
                case TxnType.Balance:
                    return Balance(funcInput);
                case TxnType.MultiTransfer:
                    return MultiTransfer(funcInput);
                case TxnType.Deposit:
                    return Deposit(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}