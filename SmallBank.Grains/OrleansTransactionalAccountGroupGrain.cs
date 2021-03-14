using System;
using Utilities;
using System.Diagnostics;
using Persist.Interfaces;
using Orleans.Concurrency;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Transactions.Abstractions;

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

    [Reentrant]
    class OrleansTransactionalAccountGroupGrain : Orleans.Grain, IOrleansTransactionalAccountGroupGrain
    {
        private readonly IPersistSingletonGroup persistSingletonGroup;
        public int numAccountPerGroup = 1;
        private readonly ITransactionalState<CustomerAccountGroup> state;

        public OrleansTransactionalAccountGroupGrain(IPersistSingletonGroup persistSingletonGroup, [TransactionalState("state")] ITransactionalState<CustomerAccountGroup> state)
        {
            this.persistSingletonGroup = persistSingletonGroup;
            this.state = state ?? throw new ArgumentNullException(nameof(state));
        }

        private int MapCustomerIdToGroup(int accountID)
        {
            return accountID / numAccountPerGroup;  // You can can also range/hash partition
        }

        public async Task SetIOCount()
        {
            persistSingletonGroup.SetIOCount();
        }

        public async Task<long> GetIOCount()
        {
            return persistSingletonGroup.GetIOCount();
        }

        private async Task<TransactionResult> Balance(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = await state.PerformRead(s => s);
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
                var success = false;
                var inputTuple = (DepositCheckingInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                await state.PerformUpdate(myState =>
                {
                    if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                    if (myState.checkingAccount.ContainsKey(id))
                    {
                        myState.checkingAccount[id] += inputTuple.Item2;
                        success = true;
                    }
                });

                if (!success)
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

        public async Task<TransactionResult> TransactSaving(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = await state.PerformUpdate(s => s);
                var inputTuple = (TransactSavingInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id))
                    {
                        ret.exception = true;
                        return ret;
                    }
                    if (myState.savingAccount[id] < inputTuple.Item2)
                    {
                        ret.exception = true;
                        return ret;
                    }
                    await state.PerformUpdate<CustomerAccountGroup>(s => s.savingAccount[id] -= inputTuple.Item2);
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
                var myState = await state.PerformRead(s => s);
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
                    var destination = GrainFactory.GetGrain<IOrleansTransactionalAccountGroupGrain>(gID);
                    var funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositChecking", funcInput);
                    task = destination.StartTransaction("DepositChecking", funcInput);
                }
                await task;
                if (task.Result.exception)
                {
                    ret.exception = true;
                    return ret;
                }
                await state.PerformUpdate<CustomerAccountGroup>(s => s.checkingAccount[id] -= inputTuple.Item3);
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
                var myState = await state.PerformRead(s => s);
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
                        await state.PerformUpdate<CustomerAccountGroup>(s => s.checkingAccount[id] -= (inputTuple.Item2 + 1));
                    else await state.PerformUpdate<CustomerAccountGroup>(s => s.checkingAccount[id] -= (inputTuple.Item2));
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
                var success = false;
                var myGroupID = -1;
                var inputTuple = (MultiTransferInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                await state.PerformUpdate(myState =>
                {
                    myGroupID = myState.GroupID;
                    if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                    if (myState.checkingAccount.ContainsKey(id) && myState.checkingAccount[id] >= inputTuple.Item2 * inputTuple.Item3.Count)
                    {
                        myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                        success = true;
                    }
                });

                if (!success)
                {
                    ret.exception = true;
                    return ret;
                }
                else
                {
                    Debug.Assert(myGroupID >= 0);
                    var destinations = inputTuple.Item3;
                    var tasks = new List<Task<TransactionResult>>();
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var funcInput = new FunctionInput(fin, new DepositCheckingInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2));
                        if (gID == myGroupID)
                        {
                            var localCall = DepositChecking(funcInput);
                            tasks.Add(localCall);
                        }
                        else
                        {
                            var destination = GrainFactory.GetGrain<IOrleansTransactionalAccountGroupGrain>(gID);
                            var task = destination.StartTransaction("DepositChecking", funcInput);
                            tasks.Add(task);
                        }
                    }
                    await Task.WhenAll(tasks);
                }
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }

        public async Task<TransactionResult> Init(FunctionInput fin)
        {
            var ret = new TransactionResult();
            try
            {
                var tuple = (InitAccountInput)fin.inputObject;
                numAccountPerGroup = tuple.Item1;
                var groupId = tuple.Item2;
                await state.PerformUpdate<CustomerAccountGroup>(s => s.GroupID = groupId);
                var minAccountID = groupId * numAccountPerGroup;
                for (int i = 0; i < numAccountPerGroup; i++)
                {
                    int accountId = minAccountID + i;
                    await state.PerformUpdate(s => s.account.Add(accountId.ToString(), accountId));
                    await state.PerformUpdate(s => s.savingAccount.Add(accountId, int.MaxValue));
                    await state.PerformUpdate(s => s.checkingAccount.Add(accountId, int.MaxValue));
                }
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }

        Task<TransactionResult> IOrleansTransactionalAccountGroupGrain.StartTransaction(string startFunction, FunctionInput inputs)
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
                case AllTxnTypes.Init:
                    return Init(inputs);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}