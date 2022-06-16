using System;
using Utilities;
using System.Diagnostics;
using Persist.Interfaces;
using Orleans.Concurrency;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Transactions.Abstractions;
using Orleans.Transactions;
using Utilities;

namespace SmallBank.Grains
{
    using InitAccountInput = Tuple<int, int>;

    using DepositInput = Tuple<Tuple<string, int>, float>;
    using MultiTransferInput = Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>;  //Source AccountID, Amount, List<Tuple<Account Name, Account ID, Grain ID>>

    using NewDepositInput = Tuple<Tuple<string, int>, float, bool>;
    using NewMultiTransferInput = Tuple<Tuple<string, int>, float, List<Tuple<string, int>>, int>;

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

        async Task<TransactionResult> Init(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var tuple = (InitAccountInput)funcInput;
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

        async Task<TransactionResult> GetBalance(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var myState = await state.PerformRead(s => s);
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
                var success = false;
                var myGroupID = -1;
                var inputTuple = (MultiTransferInput)funcInput;
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
                    var tasks = new List<Task>();
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2);
                        if (gID == myGroupID) tasks.Add(Deposit(input));
                        else
                        {
                            var destination = GrainFactory.GetGrain<IOrleansTransactionalAccountGroupGrain>(gID);
                            tasks.Add(destination.StartTransaction("Deposit", input));
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

        async Task<TransactionResult> Deposit(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var success = false;
                var inputTuple = (DepositInput)funcInput;
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

        public async Task<TransactionResult> MultiTransferNoDeadlock(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var success = false;
                var myGroupID = -1;
                var inputTuple = (MultiTransferInput)funcInput;
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
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new DepositInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2);
                        if (gID == myGroupID)
                        {
                            var task = Deposit(input);
                            await task;
                        }
                        else
                        {
                            var destination = GrainFactory.GetGrain<IOrleansTransactionalAccountGroupGrain>(gID);
                            var task = destination.StartTransaction("Deposit", input);
                            await task;
                        }
                    }
                }
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }

        async Task<TransactionResult> MultiTransferWithNOOP(object funcInput, DateTime time)
        {
            var ret = new TransactionResult();
            ret.beforeExeTime = time;
            try
            {
                var myGroupID = -1;
                var inputTuple = (NewMultiTransferInput)funcInput;   // <Source AccountID>, Amount, List<Dest AccountID>
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                var v = await state.PerformUpdate(myState =>
                {
                    var t1 = DateTime.Now;
                    myGroupID = myState.GroupID;
                    if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                    if (myState.checkingAccount.ContainsKey(id) && myState.checkingAccount[id] >= inputTuple.Item2 * inputTuple.Item3.Count)
                    {
                        myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                        return new Tuple<bool, DateTime>(true, t1);
                    }
                    else return new Tuple<bool, DateTime>(false, t1);
                });
                var success = v.Item1;
                ret.beforeUpdate1Time = v.Item2;

                if (!success)
                {
                    ret.exception = true;
                    return ret;
                }
                else
                {
                    Debug.Assert(myGroupID >= 0);
                    var destinations = inputTuple.Item3;
                    var count = 0;
                    var write = true;
                    var numWriter = inputTuple.Item4;
                    ret.callGrainTime = DateTime.Now;
                    foreach (var tuple in destinations)
                    {
                        count++;
                        if (count == numWriter) write = false;
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        var input = new NewDepositInput(new Tuple<string, int>(tuple.Item1, tuple.Item2), inputTuple.Item2, write);
                        if (gID == myGroupID)
                        {
                            var task = DepositWithNOOP(input);
                            await task;
                        }
                        else
                        {
                            var destination = GrainFactory.GetGrain<IOrleansTransactionalAccountGroupGrain>(gID);
                            var task = destination.StartTransaction("DepositWithNOOP", input);
                            await task;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                ret.exception = true;
            }
            ret.afterExeTime = DateTime.Now;
            return ret;
        }

        async Task<TransactionResult> DepositWithNOOP(object funcInput)
        {
            var ret = new TransactionResult();
            try
            {
                var inputTuple = (NewDepositInput)funcInput;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                var write = inputTuple.Item3;
                var success = false;
                if (write == false) success = true;
                else
                {
                    success = await state.PerformUpdate(myState =>
                    {
                        if (!string.IsNullOrEmpty(custName)) id = myState.account[custName];
                        if (myState.checkingAccount.ContainsKey(id))
                        {
                            myState.checkingAccount[id] += inputTuple.Item2;
                            return true;
                        }
                        else return false;
                    });
                }

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

        public Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            var time = DateTime.Now;
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
                case AllTxnTypes.MultiTransferNoDeadlock:
                    return MultiTransferNoDeadlock(funcInput);
                case AllTxnTypes.MultiTransferWithNOOP:
                    return MultiTransferWithNOOP(funcInput, time);
                case AllTxnTypes.DepositWithNOOP:
                    return DepositWithNOOP(funcInput);

                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }
    }
}