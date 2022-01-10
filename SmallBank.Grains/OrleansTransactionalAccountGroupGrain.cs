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

        private long grainKey;

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

        private async Task<TransactionResult> Balance(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = await state.PerformRead(s => s);
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

        public async Task<TransactionResult> TransactSaving(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = await state.PerformUpdate(s => s);
                var inputTuple = (TransactSavingInput)funcInput;
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

        private async Task<TransactionResult> Transfer(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = await state.PerformRead(s => s);
                var inputTuple = (TransferInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item3)
                {
                    ret.exception = true;
                    return ret;
                }
                var gID = MapCustomerIdToGroup(inputTuple.Item2.Item2);
                var input = new DepositCheckingInput(inputTuple.Item2, inputTuple.Item3);
                Task<TransactionResult> task;
                if (gID == myState.GroupID) task = DepositChecking(funcInput);
                else
                {
                    var destination = GrainFactory.GetGrain<IOrleansTransactionalAccountGroupGrain>(gID);
                    task = destination.StartTransaction("DepositChecking", input);
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

        public async Task<TransactionResult> WriteCheck(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = await state.PerformRead(s => s);
                var inputTuple = (WriteCheckInput)funcInput;
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

        /*
        public async Task<TransactionResult> MultiTransfer(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myGroupID = -1;
                var inputTuple = (MultiTransferInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                var success = await state.PerformUpdate(myState =>
                {
                    myGroupID = myState.GroupID;
                    if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                    if (myState.checkingAccount.ContainsKey(id) && myState.checkingAccount[id] >= inputTuple.Item2 * inputTuple.Item3.Count)
                    {
                        myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                        return true;
                    }
                    else return false;
                });

                if (!success)
                {
                    ret.exception = true;
                    return ret;
                }
                else
                {
                    Debug.Assert(myGroupID >= 0);
                    var tasks = new List<Task>();
                    var destinations = inputTuple.Item3;
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositCheckingInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2);
                        if (gID == myGroupID) tasks.Add(DepositChecking(input)); 
                        else
                        {
                            var destination = GrainFactory.GetGrain<IOrleansTransactionalAccountGroupGrain>(gID);
                            tasks.Add(destination.StartTransaction("DepositChecking", input));
                        }
                    }
                    await Task.WhenAll(tasks);
                }
            }
            catch (Exception e)
            {
                ret.exception = true;
            }
            return ret;
        }*/
        
        // no deadlock
        public async Task<TransactionResult> MultiTransfer(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myGroupID = -1;
                var inputTuple = (MultiTransferInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                var success = await state.PerformUpdate(myState =>
                {
                    myGroupID = myState.GroupID;
                    if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                    if (myState.checkingAccount.ContainsKey(id) && myState.checkingAccount[id] >= inputTuple.Item2 * inputTuple.Item3.Count)
                    {
                        myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                        return true;
                    }
                    else return false;
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
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositCheckingInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2);
                        if (gID == myGroupID)
                        {
                            var task = DepositChecking(input);
                            await task;
                        }
                        else
                        {
                            var destination = GrainFactory.GetGrain<IOrleansTransactionalAccountGroupGrain>(gID);
                            var task = destination.StartTransaction("DepositChecking", input);
                            await task;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                ret.exception = true;
            }
            return ret;
        }
        
        private async Task<TransactionResult> DepositChecking(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var inputTuple = (DepositCheckingInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                var success = await state.PerformUpdate(myState =>
                {
                    if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                    if (myState.checkingAccount.ContainsKey(id))
                    {
                        myState.checkingAccount[id] += inputTuple.Item2;
                        return true;
                    }
                    else return false;
                });

                if (!success)
                {
                    ret.exception = true;
                    return ret;
                }
            }
            catch (Exception e)
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
                var tuple = (InitAccountInput)funcInput;
                numAccountPerGroup = tuple.Item1;
                var groupId = tuple.Item2;
                grainKey = groupId;
                var minAccountID = groupId * numAccountPerGroup;
                await state.PerformUpdate(s =>
                {
                    s.GroupID = groupId;
                    for (int i = 0; i < numAccountPerGroup; i++)
                    {
                        int accountId = minAccountID + i;
                        s.account.Add(accountId.ToString(), accountId);
                        s.savingAccount.Add(accountId, int.MaxValue);
                        s.checkingAccount.Add(accountId, int.MaxValue);
                    }
                });
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }

        Task<TransactionResult> IOrleansTransactionalAccountGroupGrain.StartTransaction(string startFunc, object funcInput)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case AllTxnTypes.Balance:
                    return Balance(funcInput);
                case AllTxnTypes.DepositChecking:
                    return DepositChecking(funcInput);
                case AllTxnTypes.TransactSaving:
                    return TransactSaving(funcInput);
                case AllTxnTypes.WriteCheck:
                    return WriteCheck(funcInput);
                case AllTxnTypes.Transfer:
                    return Transfer(funcInput);
                case AllTxnTypes.MultiTransfer:
                    //Console.WriteLine($"AccountGrain {grainKey}: StartTransaction MultiTransfer!!!!!!!!!!!!!!!!!!!");
                    return MultiTransfer(funcInput);
                case AllTxnTypes.Init:
                    return Init(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}