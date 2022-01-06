using System;
using Orleans;
using Utilities;
using System.Diagnostics;
using Persist.Interfaces;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Implementation.Logging;

namespace Concurrency.Implementation
{
    [GrainPlacementStrategy]
    public abstract class TransactionExecutionGrain<TState> : Grain, ITransactionExecutionGrain where TState : ICloneable, new()
    {
        private int myID;
        private int coordID;
        private int highestCommittedBid;
        private readonly string myClassName;
        private Tuple<int, string> myFullID;
        private TransactionScheduler myScheduler;
        protected ITransactionalState<TState> state;
        private Dictionary<int, int> coordinatorMap;    // <act tid, grainID who starts the act>
        protected ILoggingProtocol<TState> log = null;
        private IGlobalTransactionCoordinatorGrain myCoord;
        private Dictionary<int, FunctionResult> funcResults;
        private readonly IPersistSingletonGroup persistSingletonGroup;
        private Dictionary<int, TaskCompletionSource<bool>> batchCommit;
        private TimeSpan deadlockTimeout = TimeSpan.FromMilliseconds(20);
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        private Dictionary<int, IGlobalTransactionCoordinatorGrain> coordList;  // <coordID, coord>

        private int maxBeforeBidOnGrain;

        public TransactionExecutionGrain(IPersistSingletonGroup persistSingletonGroup, string myClassName)
        {
            this.persistSingletonGroup = persistSingletonGroup;
            this.myClassName = myClassName;
        }

        public async override Task OnActivateAsync()
        {
            maxBeforeBidOnGrain = -1;
            myID = (int)this.GetPrimaryKeyLong();
            myFullID = new Tuple<int, string>(myID, myClassName);
            // <nonDetCCType, loggingConfig, numCoord>
            coordID = myID % Constants.numCoordPerSilo;
            myCoord = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(coordID);
            state = new HybridState<TState>(Constants.ccType);

            switch (Constants.loggingType)
            {
                case LoggingType.NOLOGGING:
                    break;
                case LoggingType.ONGRAIN:
                    log = new Simple2PCLoggingProtocol<TState>(GetType().ToString(), myID);
                    break;
                case LoggingType.PERSISTGRAIN:
                    var persistGrainID = Helper.MapGrainIDToPersistItemID(Constants.numPersistItemPerSilo, myID);
                    var persistGrain = GrainFactory.GetGrain<IPersistGrain>(persistGrainID);
                    log = new Simple2PCLoggingProtocol<TState>(GetType().ToString(), myID, persistGrain);
                    break;
                case LoggingType.PERSISTSINGLETON:
                    var persistWorkerID = Helper.MapGrainIDToPersistItemID(Constants.numPersistItemPerSilo, myID);
                    var persistWorker = persistSingletonGroup.GetSingleton(persistWorkerID);
                    log = new Simple2PCLoggingProtocol<TState>(GetType().ToString(), myID, persistWorker);
                    break;
                default:
                    throw new Exception($"Exception: Unknown loggingType {Constants.loggingType}");
            }

            highestCommittedBid = -1;
            coordinatorMap = new Dictionary<int, int>();
            funcResults = new Dictionary<int, FunctionResult>();
            batchScheduleMap = new Dictionary<int, DeterministicBatchSchedule>();
            myScheduler = new TransactionScheduler(batchScheduleMap);
            batchCommit = new Dictionary<int, TaskCompletionSource<bool>>();
            coordList = new Dictionary<int, IGlobalTransactionCoordinatorGrain>();
            coordList.Add(coordID, myCoord);
        }

        /**
         * Submit a determinictic transaction to the coordinator. 
         * On receiving the returned transaction context, start the execution of a transaction.
         * 
         */
        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput, Dictionary<int, Tuple<string, int>> grainAccessInfo)
        {
            var context = await myCoord.NewTransaction(grainAccessInfo);
            if (highestCommittedBid < context.highestCommittedBid) highestCommittedBid = context.highestCommittedBid;
            var c1 = new FunctionCall(startFunc, funcInput, GetType());
            var t1 = Execute(c1, context);
            await t1;
            if (highestCommittedBid < context.bid)
            {
                Debug.Assert(batchCommit.ContainsKey(context.bid));
                await batchCommit[context.bid].Task;
            }
            funcResults.Remove(context.tid);
            var res = new TransactionResult(false, t1.Result.resultObject);  // PACT never abort
            res.isDet = true;
            return res;
        }

        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            TransactionContext context = null;
            Task<FunctionResult> t1 = null;
            var canCommit = true;
            var res = new TransactionResult();
            try
            {
                context = await myCoord.NewTransaction();
                if (highestCommittedBid < context.highestCommittedBid) highestCommittedBid = context.highestCommittedBid;
                context.coordID = myID;
                var c1 = new FunctionCall(startFunc, funcInput, GetType());
                t1 = Execute(c1, context);
                await t1;
                canCommit = !t1.Result.exception;
                Debug.Assert(t1.Result.grainsInNestedFunctions.ContainsKey(myID));
                var maxBeforeBid = -1;
                if (canCommit)
                {
                    /*
                     var result = await Prepare_2PC(context.tid, myID, t1.Result.grainsInNestedFunctions, res);
                     canCommit = result.Item1;
                     maxBeforeBid = result.Item2;*/

                    maxBeforeBid = t1.Result.maxBeforeBid;
                    var result = CheckSerializability(t1.Result);
                    canCommit = result.Item1;
                    if (canCommit) canCommit = await Prepare_2PC(context.tid, myID, t1.Result);
                    else
                    {
                        if (result.Item2) res.Exp_Serializable = true;
                        else res.Exp_NotSureSerializable = true;
                    }
                }
                else res.Exp_Deadlock |= t1.Result.Exp_Deadlock;  // when deadlock = false, exception may from RW conflict

                if (canCommit) await Commit_2PC(context.tid, maxBeforeBid, t1.Result.grainsInNestedFunctions);
                else
                {
                    res.exception = true;
                    await Abort_2PC(context.tid, t1.Result);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n Exception(StartTransaction)::{myID}: transaction {startFunc} {context.tid} exception {e.Message}, {e.StackTrace}");
            }
            if (canCommit && highestCommittedBid < t1.Result.maxBeforeBid)
            {
                var grainID = t1.Result.grainWithHighestBeforeBid;
                if (grainID == myFullID) await WaitForBatchCommit(t1.Result.maxBeforeBid);
                else
                {
                    var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.Item1, grainID.Item2);
                    var new_bid = await grain.WaitForBatchCommit(t1.Result.maxBeforeBid);
                    if (highestCommittedBid < new_bid) highestCommittedBid = new_bid;
                }
            }
            return res;
        }

        public async Task<bool> PrepareA(int tid, bool doLogging)
        {
            if (state == null) return true;  // Stateless grain always vote "yes" for 2PC
            var prepareResult = await state.Prepare(tid);
            if (prepareResult && log != null && doLogging) await log.HandleOnPrepareIn2PC(state, tid, coordinatorMap[tid]);
            return prepareResult;
        }

        private async Task<bool> Prepare_2PC(int tid, int coordinatorKey, FunctionResult result)
        {
            var grainIDsInTransaction = new HashSet<int>();
            grainIDsInTransaction.UnionWith(result.grainsInNestedFunctions.Keys);
            var hasException = result.exception;
            var canCommit = !hasException;
            if (!hasException)
            {
                var logTask = Task.CompletedTask;
                if (log != null) logTask = log.HandleBeforePrepareIn2PC(tid, coordinatorKey, grainIDsInTransaction);

                var prepareResult = new List<Task<bool>>();
                foreach (var item in result.grainsInNestedFunctions)
                {
                    if (item.Key == myID) prepareResult.Add(PrepareA(tid, !item.Value.Item2));
                    else prepareResult.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, item.Value.Item1).PrepareA(tid, !item.Value.Item2));
                }
                await Task.WhenAll(logTask, Task.WhenAll(prepareResult));
                foreach (var vote in prepareResult)
                {
                    if (vote.Result == false)
                    {
                        canCommit = false;
                        break;
                    }
                }
            }
            return canCommit;
        }

        /**
         * On receive the schedule for a specific batch
         * 1. Store this schedule.
         * 2. Check if there is function call that should be executed now, and execute it if yes.
         */
        public Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule)
        {
            if (highestCommittedBid < schedule.highestCommittedBid) highestCommittedBid = schedule.highestCommittedBid;
            else schedule.highestCommittedBid = highestCommittedBid;
            batchCommit.Add(schedule.bid, new TaskCompletionSource<bool>());
            myScheduler.ackBatchCommit(highestCommittedBid);
            batchScheduleMap.Add(schedule.bid, schedule);
            myScheduler.RegisterDeterministicBatchSchedule(schedule.bid);
            return Task.CompletedTask;
        }

        public async Task<int> WaitForBatchCommit(int bid)
        {
            if (highestCommittedBid >= bid) return highestCommittedBid;
            await batchCommit[bid].Task;
            return highestCommittedBid;
        }

        public Task AckBatchCommit(int bid)
        {
            if (highestCommittedBid < bid) highestCommittedBid = bid;
            batchCommit[bid].SetResult(true);
            batchCommit.Remove(bid);
            return Task.CompletedTask;
        }

        public async Task<TState> GetState(TransactionContext context, AccessMode mode)
        {
            switch (mode)
            {
                case AccessMode.Read:
                    funcResults[context.tid].isReadOnlyOnGrain = true;
                    return await state.Read(context);
                case AccessMode.ReadWrite:
                    return await state.ReadWrite(context);
                default:
                    throw new Exception("Exception: Unknown access mode. ");
            }
        }

        public async Task<TransactionResult> CallGrain(TransactionContext context, int grainID, string grainNameSpace, FunctionCall call)
        {
            var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID, grainNameSpace);
            var res = await grain.Execute(call, context);
            funcResults[context.tid].mergeWithFunctionResult(res);
            return new TransactionResult(res.exception, res.resultObject);
        }

        /**
         *Allow reentrance to enforce ordered execution
         */
        public async Task<FunctionResult> Execute(FunctionCall call, TransactionContext context)
        {
            var tid = context.tid;
            if (funcResults.ContainsKey(tid) == false) funcResults.Add(tid, new FunctionResult());
            var res = funcResults[tid];
            if (context.isDet == false)
            {
                try
                {
                    var t = myScheduler.waitForTurn(tid);
                    await Task.WhenAny(t, Task.Delay(deadlockTimeout));
                    if (t.IsCompleted)
                    {
                        var txnRes = await InvokeFunction(call, context);
                        res.exception |= txnRes.exception;
                        res.resultObject = txnRes.resultObject;
                        updateExecutionResult(tid, res);
                    }
                    else
                    {
                        res.Exp_Deadlock = true;
                        res.exception = true;
                        updateExecutionResult(res);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"\n Exception::InvokeFunction: {e.Message}");
                }
                return res;
            }
            else
            {
                var bid = context.bid;
                var myTurnIndex = await myScheduler.waitForTurn(bid, tid);
                // Execute the function call;
                var txnRes = await InvokeFunction(call, context);
                res.exception = txnRes.exception;
                res.resultObject = txnRes.resultObject;
                if (myScheduler.ackComplete(bid, tid, myTurnIndex))
                {
                    // The scheduler has switched batches, need to commit now
                    var coordID = batchScheduleMap[bid].coordID;
                    if (log != null && res.isReadOnlyOnGrain == false) await log.HandleOnCompleteInDeterministicProtocol(state, bid, coordID);

                    IGlobalTransactionCoordinatorGrain coordinator;
                    if (coordList.ContainsKey(coordID) == false)
                    {
                        coordinator = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(coordID);
                        coordList.Add(coordID, coordinator);
                    }
                    else coordinator = coordList[coordID];
                    _ = coordinator.AckBatchCompletion(bid);
                }
                return res;
            }
        }

        private void updateExecutionResult(FunctionResult res)
        {
            if (res.grainsInNestedFunctions.ContainsKey(myID) == false)
                res.grainsInNestedFunctions.Add(myID, new Tuple<string, bool>(myClassName, true));
        }

        // Update the metadata of the execution results, including accessed grains, before/after set, etc.
        private void updateExecutionResult(int tid, FunctionResult res)
        {
            if (res.grainWithHighestBeforeBid.Item1 == -1) res.grainWithHighestBeforeBid = myFullID;

            if (res.grainsInNestedFunctions.ContainsKey(myID) == false)
                res.grainsInNestedFunctions.Add(myID, new Tuple<string, bool>(myClassName, res.isReadOnlyOnGrain));

            var result = myScheduler.getBeforeAfter(tid);   // <maxBeforeBid, minAfterBid, isConsecutive>
            var maxBeforeBid = result.Item1;
            if (maxBeforeBidOnGrain > maxBeforeBid) maxBeforeBid = maxBeforeBidOnGrain;
            res.setSchedulingStatistics(maxBeforeBid, result.Item2, result.Item3, myFullID);
        }

        private async Task<TransactionResult> InvokeFunction(FunctionCall call, TransactionContext context)
        {
            var key = context.isDet ? context.bid : context.tid;
            if (!context.isDet) coordinatorMap.Add(key, context.coordID);
            var mi = call.grainClassName.GetMethod(call.funcName);
            var t = (Task<TransactionResult>)mi.Invoke(this, new object[] { context, call.funcInput });
            return await t;
        }

        // <isCommit, maxBeforeBid>
        private async Task<Tuple<bool, int>> Prepare_2PC(int tid, int coordinatorKey, Dictionary<int, Tuple<string, bool>> grainsInNestedFunctions, TransactionResult res)
        {
            var grainIDsInTransaction = new HashSet<int>();
            grainIDsInTransaction.UnionWith(grainsInNestedFunctions.Keys);
            var logTask = Task.CompletedTask;
            if (log != null) logTask = log.HandleBeforePrepareIn2PC(tid, coordinatorKey, grainIDsInTransaction);

            var prepareResult = new List<Task<Tuple<bool, int, int, bool>>>();
            foreach (var item in grainsInNestedFunctions)
            {
                if (item.Key == myID) prepareResult.Add(Prepare(tid, !item.Value.Item2));
                else prepareResult.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, item.Value.Item1).Prepare(tid, !item.Value.Item2));
            }
            await Task.WhenAll(logTask, Task.WhenAll(prepareResult));

            var maxBefore = -1;
            var minAfter = -1;
            var isConsecutive = true;
            foreach (var vote in prepareResult)
            {
                if (vote.Result.Item1 == false) return new Tuple<bool, int>(false, -1);
                if (vote.Result.Item2 != -1)
                {
                    if (maxBefore == -1) maxBefore = vote.Result.Item2;
                    else maxBefore = Math.Max(maxBefore, vote.Result.Item2);
                }
                if (vote.Result.Item3 != -1)
                {
                    if (minAfter == -1) minAfter = vote.Result.Item3;
                    else minAfter = Math.Min(minAfter, vote.Result.Item3);
                }
                isConsecutive &= vote.Result.Item4;
            }

            // check serializability
            if (maxBefore <= highestCommittedBid) return new Tuple<bool, int>(true, maxBefore);
            if (isConsecutive && maxBefore < minAfter) return new Tuple<bool, int>(true, maxBefore);
            if (maxBefore >= minAfter)
            {
                res.exception = true;
                res.Exp_Serializable = true;
                return new Tuple<bool, int>(false, -1);
            }
            res.exception = true;
            res.Exp_NotSureSerializable = true;
            return new Tuple<bool, int>(false, -1);
        }

        // serializable or not, sure or not sure
        public Tuple<bool, bool> CheckSerializability(FunctionResult result)
        {
            if (result.maxBeforeBid <= highestCommittedBid) return new Tuple<bool, bool>(true, true);
            if (result.isBeforeAfterConsecutive && result.maxBeforeBid < result.minAfterBid) return new Tuple<bool, bool>(true, true);
            if (result.maxBeforeBid >= result.minAfterBid && result.minAfterBid != -1) return new Tuple<bool, bool>(false, true);
            return new Tuple<bool, bool>(false, false);
        }

        private async Task Commit_2PC(int tid, int maxBeforeBid, Dictionary<int, Tuple<string, bool>> grainsInNestedFunctions)
        {
            var commitTasks = new List<Task>();
            if (log != null) commitTasks.Add(log.HandleOnCommitIn2PC(tid, coordinatorMap[tid]));
            foreach (var item in grainsInNestedFunctions)
            {
                if (item.Key == myID) commitTasks.Add(Commit(tid, maxBeforeBid, !item.Value.Item2));
                else commitTasks.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, item.Value.Item1).Commit(tid, maxBeforeBid, !item.Value.Item2));
            }
            await Task.WhenAll(commitTasks);
        }

        private async Task Abort_2PC(int tid, FunctionResult result)
        {
            var abortTasks = new List<Task>();
            //Presume Abort
            foreach (var item in result.grainsInNestedFunctions)
            {
                if (item.Key == myID) abortTasks.Add(Abort(tid));
                else abortTasks.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, item.Value.Item1).Abort(tid));
            }
            await Task.WhenAll(abortTasks);
        }

        public async Task Abort(int tid)
        {
            if (state == null) return;
            var tasks = new List<Task>();
            tasks.Add(state.Abort(tid));
            myScheduler.ackComplete(tid);
            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        public async Task Commit(int tid, int maxBeforeBid, bool doLogging)
        {
            if (state == null) return;

            //Debug.Assert(maxBeforeBidOnGrain <= maxBeforeBid);
            //maxBeforeBidOnGrain = maxBeforeBid;

            var tasks = new List<Task>();
            tasks.Add(state.Commit(tid));
            if (log != null && doLogging) tasks.Add(log.HandleOnCommitIn2PC(tid, coordinatorMap[tid]));
            myScheduler.ackComplete(tid);
            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        // <vote, maxBeforeBid, minAfterBid, isConsecutive>
        public async Task<Tuple<bool, int, int, bool>> Prepare(int tid, bool doLogging)
        {
            if (state == null) return new Tuple<bool, int, int, bool>(true, -1, -1, true);  // Stateless grain always vote "yes" for 2PC
            var prepareResult = await state.Prepare(tid);
            if (prepareResult && log != null && doLogging) await log.HandleOnPrepareIn2PC(state, tid, coordinatorMap[tid]);
            if (prepareResult == false) return new Tuple<bool, int, int, bool>(false, -1, -1, true);

            // get maxBefore and minAfter
            var result = myScheduler.getBeforeAfter(tid);
            var maxBefore = Math.Max(result.Item1, maxBeforeBidOnGrain);
            return new Tuple<bool, int, int, bool>(true, maxBefore, result.Item2, result.Item3);
        }

        private void Cleanup(int tid)
        {
            funcResults.Remove(tid);
            coordinatorMap.Remove(tid);
        }
    }
}