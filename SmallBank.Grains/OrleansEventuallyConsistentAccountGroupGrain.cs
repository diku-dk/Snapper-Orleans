using System;
using Utilities;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;

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

    class OrleansEventuallyConsistentAccountGroupGrain : Orleans.Grain, IOrleansEventuallyConsistentAccountGroupGrain
    {
        CustomerAccountGroup state = new CustomerAccountGroup();
        public int numAccountPerGroup = 1;

        private int MapCustomerIdToGroup(int accountID)
        {
            return accountID / numAccountPerGroup; //You can can also range/hash partition
        }

        private async Task<TransactionResult> Balance(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var custName = (BalanceInput)fin.inputObject;
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

        private async Task<TransactionResult> DepositChecking(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var inputTuple = (DepositCheckingInput)fin.inputObject;
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

        public async Task<TransactionResult> TransactSaving(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var inputTuple = (TransactSavingInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id) || myState.savingAccount[id] < inputTuple.Item2)
                    {
                        ret.exception = true;
                        return ret;
                    }
                    myState.savingAccount[id] -= inputTuple.Item2;
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

        private async Task<TransactionResult> Transfer(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var inputTuple = (TransferInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item3)
                {
                    ret.exception = true;
                    return ret;
                }
                var gID = MapCustomerIdToGroup(inputTuple.Item2.Item2);
                var funcInput = new FunctionInput(fin, new DepositCheckingInput(inputTuple.Item2, inputTuple.Item3));
                Task<TransactionResult> task;
                if (gID == myState.GroupID) task = DepositChecking(funcInput);
                else
                {
                    var destination = GrainFactory.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(gID);
                    var funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositChecking", funcInput);
                    task = destination.StartTransaction("DepositChecking", funcInput);
                }
                await task;
                if (task.Result.exception) return ret;
                myState.checkingAccount[id] -= inputTuple.Item3;
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }

        public async Task<TransactionResult> WriteCheck(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var inputTuple = (WriteCheckInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        ret.exception = true;
                        return ret;
                    }
                    if (myState.savingAccount[id] + myState.checkingAccount[id] < inputTuple.Item2)
                        myState.checkingAccount[id] -= (inputTuple.Item2 + 1); //Pay a penalty  
                    else myState.checkingAccount[id] -= inputTuple.Item2;
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

        public async Task<TransactionResult> MultiTransfer(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
                var inputTuple = (MultiTransferInput)fin.inputObject;
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
                        var funcInput = new FunctionInput(fin, new DepositCheckingInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2));
                        if (gID == myState.GroupID)
                        {
                            var localCall = DepositChecking(funcInput);
                            tasks.Add(localCall);
                        }
                        else
                        {
                            var destination = this.GrainFactory.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(gID);
                            var task = destination.StartTransaction("DepositChecking", funcInput);
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

        public async Task<TransactionResult> InitBankAccounts(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = state;
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
                ret.exception = true;
            }
            return ret;
        }

        Task<TransactionResult> IOrleansEventuallyConsistentAccountGroupGrain.StartTransaction(string startFunction, FunctionInput inputs)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunction.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunction}");
            switch (fnType)
            {
                case AllTxnTypes.Balance:
                    return Balance(inputs);
                case AllTxnTypes.DepositChecking:
                    return DepositChecking(inputs);
                case AllTxnTypes.TransactSaving:
                    return TransactSaving(inputs);
                case AllTxnTypes.WriteCheck:
                    return WriteCheck(inputs);
                case AllTxnTypes.Transfer:
                    return Transfer(inputs);
                case AllTxnTypes.MultiTransfer:
                    return MultiTransfer(inputs);
                case AllTxnTypes.InitBankAccounts:
                    return InitBankAccounts(inputs);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}
