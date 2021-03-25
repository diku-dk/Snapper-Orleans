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
        protected readonly Tuple<int, string> myGrainID;
        private TransactionScheduler myScheduler;
        protected ITransactionalState<TState> state;
        private Dictionary<int, int> coordinatorMap;    // <act tid, grainID who starts the act>
        protected ILoggingProtocol<TState> log = null;
        private IGlobalTransactionCoordinatorGrain myCoordinator;
        private readonly IPersistSingletonGroup persistSingletonGroup;
        private Dictionary<int, TaskCompletionSource<bool>> batchCommit;
        private TimeSpan deadlockTimeout = TimeSpan.FromMilliseconds(20);
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        private Dictionary<int, IGlobalTransactionCoordinatorGrain> coordList;  // <coordID, coord>

        public TransactionExecutionGrain(IPersistSingletonGroup persistSingletonGroup, string grainClassName)
        {
            this.persistSingletonGroup = persistSingletonGroup;
            myGrainID = new Tuple<int, string>(myID, grainClassName);
        }

        public async override Task OnActivateAsync()
        {
            myID = (int)this.GetPrimaryKeyLong();
            var configTuple = await GrainFactory.GetGrain<IConfigurationManagerGrain>(0).GetConfiguration();
            // <nonDetCCType, loggingConfig, numCoord>
            coordID = Helper.MapGrainIDToCoordID(configTuple.Item3, myID);
            myCoordinator = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(coordID);
            state = new HybridState<TState>(configTuple.Item1);
            var loggingConfig = configTuple.Item2;
            switch (loggingConfig.loggingType)
            {
                case LoggingType.NOLOGGING:
                    break;
                case LoggingType.ONGRAIN:
                    log = new Simple2PCLoggingProtocol<TState>(GetType().ToString(), myID, loggingConfig);
                    break;
                case LoggingType.PERSISTGRAIN:
                    var persistGrainID = Helper.MapGrainIDToPersistItemID(loggingConfig.numPersistItem, myID);
                    var persistGrain = GrainFactory.GetGrain<IPersistGrain>(persistGrainID);
                    log = new Simple2PCLoggingProtocol<TState>(GetType().ToString(), myID, loggingConfig, persistGrain);
                    break;
                case LoggingType.PERSISTSINGLETON:
                    var persistWorkerID = Helper.MapGrainIDToPersistItemID(loggingConfig.numPersistItem, myID);
                    var persistWorker = persistSingletonGroup.GetSingleton(persistWorkerID);
                    log = new Simple2PCLoggingProtocol<TState>(GetType().ToString(), myID, loggingConfig, persistWorker);
                    break;
                default:
                    throw new Exception($"Exception: Unknown loggingType {loggingConfig.loggingType}");
            }

            highestCommittedBid = -1;
            coordinatorMap = new Dictionary<int, int>();
            batchScheduleMap = new Dictionary<int, DeterministicBatchSchedule>();
            myScheduler = new TransactionScheduler(batchScheduleMap, myID);
            batchCommit = new Dictionary<int, TaskCompletionSource<bool>>();
            coordList = new Dictionary<int, IGlobalTransactionCoordinatorGrain>();
            coordList.Add(coordID, myCoordinator);
        }

        /**
         * Submit a determinictic transaction to the coordinator. 
         * On receiving the returned transaction context, start the execution of a transaction.
         * 
         */
        public async Task<TransactionResult> StartTransaction(Dictionary<Tuple<int, string>, int> grainAccessInformation, string startFunction, FunctionInput inputs)
        {
            var context = await myCoordinator.NewTransaction(grainAccessInformation);
            if (highestCommittedBid < context.highestBatchIdCommitted) highestCommittedBid = context.highestBatchIdCommitted;
            inputs.context = context;
            var c1 = new FunctionCall(GetType(), startFunction, inputs);
            var t1 = Execute(c1);
            await t1;
            if (highestCommittedBid < context.batchID)
            {
                Debug.Assert(batchCommit.ContainsKey(context.batchID));
                await batchCommit[context.batchID].Task;
            }
            var res = new TransactionResult(false, t1.Result.resultObject);  // PACT never abort
            res.isDet = true;
            return res;
        }

        public async Task<TransactionResult> StartTransaction(string startFunction, FunctionInput functionCallInput)
        {
            TransactionContext context = null;
            Task<FunctionResult> t1 = null;
            var canCommit = true;
            var res = new TransactionResult();
            try
            {
                context = await myCoordinator.NewTransaction();
                if (highestCommittedBid < context.highestBatchIdCommitted) highestCommittedBid = context.highestBatchIdCommitted;
                functionCallInput.context = context;
                context.coordinatorKey = myID;
                var c1 = new FunctionCall(GetType(), startFunction, functionCallInput);
                t1 = Execute(c1);
                await t1;
                canCommit = !t1.Result.hasException();
                Debug.Assert(t1.Result.grainsInNestedFunctions.ContainsKey(myGrainID));
                if (canCommit)
                {
                    var result = CheckSerializability(t1.Result);
                    canCommit = result.Item1;
                    if (canCommit) canCommit = await Prepare_2PC(context.transactionID, myID, t1.Result);
                    else
                    {
                        if (result.Item2) res.Exp_Serializable = true;
                        else res.Exp_NotSureSerializable = true;
                    } 
                }
                else res.Exp_Deadlock |= t1.Result.Exp_Deadlock;  // when deadlock = false, exception may from RW conflict

                if (canCommit) await Commit_2PC(context.transactionID, t1.Result);
                else
                {
                    res.exception = true;
                    await Abort_2PC(context.transactionID, t1.Result);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n Exception(StartTransaction)::{myID}: transaction {startFunction} {context.transactionID} exception {e.Message}, {e.StackTrace}");
            }
            if (canCommit && t1.Result.beforeSet.Count != 0 && highestCommittedBid < t1.Result.maxBeforeBid)
            {
                var grainID = t1.Result.grainWithHighestBeforeBid;
                if (grainID == myGrainID) await WaitForBatchCommit(t1.Result.maxBeforeBid);
                else
                {
                    var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.Item1, grainID.Item2);
                    var new_bid = await grain.WaitForBatchCommit(t1.Result.maxBeforeBid);
                    if (highestCommittedBid < new_bid) highestCommittedBid = new_bid;
                }
            }
            return res;
        }

        /**
         * On receive the schedule for a specific batch
         * 1. Store this schedule.
         * 2. Check if there is function call that should be executed now, and execute it if yes.
         */
        public Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule)
        {
            if (highestCommittedBid < schedule.highestCommittedBatchId) highestCommittedBid = schedule.highestCommittedBatchId;
            else schedule.highestCommittedBatchId = highestCommittedBid;
            batchCommit.Add(schedule.batchID, new TaskCompletionSource<bool>());
            myScheduler.ackBatchCommit(highestCommittedBid);
            batchScheduleMap.Add(schedule.batchID, schedule);
            myScheduler.RegisterDeterministicBatchSchedule(schedule.batchID);
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

        /**
         *Allow reentrance to enforce ordered execution
         */
        public async Task<FunctionResult> Execute(FunctionCall call)
        {
            var tid = call.funcInput.context.transactionID;
            if (call.funcInput.context.isDeterministic == false)
            {
                FunctionResult invokeRet = null;
                try
                {
                    var t = myScheduler.waitForTurn(tid);
                    await Task.WhenAny(t, Task.Delay(deadlockTimeout));
                    if (t.IsCompleted)
                    {
                        invokeRet = await InvokeFunction(call);
                        updateExecutionResult(tid, invokeRet);
                    }
                    else
                    {
                        invokeRet = new FunctionResult();
                        invokeRet.Exp_Deadlock = true;
                        invokeRet.setException();
                        updateExecutionResult(invokeRet);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine($"\n Exception::InvokeFunction: {e.Message}");
                }
                return invokeRet;
            }
            else
            {
                var bid = call.funcInput.context.batchID;
                var myTurnIndex = await myScheduler.waitForTurn(bid, tid);
                //Execute the function call;
                var ret = await InvokeFunction(call);
                if (myScheduler.ackComplete(bid, tid, myTurnIndex))
                {
                    //The scheduler has switched batches, need to commit now
                    var coordID = batchScheduleMap[bid].globalCoordinator;
                    if (log != null) await log.HandleOnCompleteInDeterministicProtocol(state, bid, coordID);  

                    IGlobalTransactionCoordinatorGrain coordinator;
                    if (coordList.ContainsKey(coordID) == false)
                    {
                        coordinator = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(coordID);
                        coordList.Add(coordID, coordinator);
                    }
                    else coordinator = coordList[coordID];
                    _ = coordinator.AckBatchCompletion(bid);
                    //Console.WriteLine($"grain {myID}: ack coord batch {bid}");
                }
                return ret;
            }
        }

        private void updateExecutionResult(FunctionResult invokeRet)
        {
            if (invokeRet.grainsInNestedFunctions.ContainsKey(myGrainID) == false)
                invokeRet.grainsInNestedFunctions.Add(myGrainID, true);
        }

        //Update the metadata of the execution results, including accessed grains, before/after set, etc.
        private void updateExecutionResult(int tid, FunctionResult invokeRet)
        {
            if (invokeRet.grainWithHighestBeforeBid.Item1 == -1) invokeRet.grainWithHighestBeforeBid = myGrainID;

            int maxBeforeBid, minAfterBid;
            bool isBeforeAfterConsecutive;

            if (invokeRet.grainsInNestedFunctions.ContainsKey(myGrainID) == false)
                invokeRet.grainsInNestedFunctions.Add(myGrainID, invokeRet.isReadOnlyOnGrain);

            var beforeSet = myScheduler.getBeforeSet(tid, out maxBeforeBid);
            var afterSet = myScheduler.getAfterSet(maxBeforeBid, out minAfterBid);
            invokeRet.beforeSet.UnionWith(beforeSet);
            invokeRet.afterSet.UnionWith(afterSet);
            if (minAfterBid == int.MaxValue) isBeforeAfterConsecutive = false;
            else if (maxBeforeBid == int.MinValue) isBeforeAfterConsecutive = true;
            else if (batchScheduleMap.ContainsKey(minAfterBid) && batchScheduleMap[minAfterBid].lastBatchID == maxBeforeBid) isBeforeAfterConsecutive = true;
            else isBeforeAfterConsecutive = false;
            invokeRet.setSchedulingStatistics(maxBeforeBid, minAfterBid, isBeforeAfterConsecutive, myGrainID);
        }

        private async Task<FunctionResult> InvokeFunction(FunctionCall call)
        {
            var context = call.funcInput.context;
            var key = (context.isDeterministic) ? context.batchID : context.transactionID;
            if (!context.isDeterministic) coordinatorMap.Add(key, context.coordinatorKey);
            var functionCallInput = call.funcInput;
            var mi = call.type.GetMethod(call.func);
            var t = (Task<FunctionResult>)mi.Invoke(this, new object[] { functionCallInput });
            await t;
            return t.Result;
        }

        // serializable or not, sure or not sure
        public Tuple<bool, bool> CheckSerializability(FunctionResult result)   
        {
            if (result.beforeSet.Count == 0) return new Tuple<bool, bool>(true, true);
            if (result.maxBeforeBid <= highestCommittedBid) return new Tuple<bool, bool>(true, true);
            if (result.isBeforeAfterConsecutive && result.maxBeforeBid < result.minAfterBid) return new Tuple<bool, bool>(true, true);
            if (result.maxBeforeBid >= result.minAfterBid) return new Tuple<bool, bool>(false, true);
            return new Tuple<bool, bool>(false, false);
        }

        private async Task<bool> Prepare_2PC(int tid, int coordinatorKey, FunctionResult result)
        {
            var grainIDsInTransaction = new HashSet<Tuple<int, string>>();
            grainIDsInTransaction.UnionWith(result.grainsInNestedFunctions.Keys);
            var hasException = result.hasException();
            var canCommit = !hasException;
            if (!hasException)
            {
                var logTask = Task.CompletedTask;
                if (log != null) logTask = log.HandleBeforePrepareIn2PC(tid, coordinatorKey, grainIDsInTransaction);

                var prepareResult = new List<Task<bool>>();
                foreach (var item in result.grainsInNestedFunctions)
                {
                    if (item.Key == myGrainID) prepareResult.Add(Prepare(tid, !item.Value));
                    else prepareResult.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key.Item1, item.Key.Item2).Prepare(tid, !item.Value));
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

        private async Task Commit_2PC(int tid, FunctionResult result)
        {
            var commitTasks = new List<Task>();
            if (log != null) commitTasks.Add(log.HandleOnCommitIn2PC(tid, coordinatorMap[tid]));
            foreach (var item in result.grainsInNestedFunctions)
            {
                if (item.Key == myGrainID) commitTasks.Add(Commit(tid, !item.Value));
                else commitTasks.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key.Item1, item.Key.Item2).Commit(tid, !item.Value));
            }
            await Task.WhenAll(commitTasks);
        }

        private async Task Abort_2PC(int tid, FunctionResult result)
        {
            var abortTasks = new List<Task>();
            //Presume Abort
            foreach (var item in result.grainsInNestedFunctions)
            {
                if (item.Key == myGrainID) abortTasks.Add(Abort(tid));
                else abortTasks.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key.Item1, item.Key.Item2).Abort(tid));
            }
            await Task.WhenAll(abortTasks);
        }

        public async Task Abort(int tid)
        {
            if (state == null) return;
            var tasks = new List<Task>();
            tasks.Add(state.Abort(tid));
            myScheduler.ackComplete((int)tid);
            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        public async Task Commit(int tid, bool doLogging)
        {
            if (state == null) return;
            var tasks = new List<Task>();
            tasks.Add(state.Commit(tid));
            if (log != null && doLogging) tasks.Add(log.HandleOnCommitIn2PC(tid, coordinatorMap[tid]));
            myScheduler.ackComplete(tid);
            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        public async Task<bool> Prepare(int tid, bool doLogging)
        {
            if (state == null) return true;  // Stateless grain always vote "yes" for 2PC
            var prepareResult = await state.Prepare(tid);
            if (prepareResult && log != null && doLogging) await log.HandleOnPrepareIn2PC(state, tid, coordinatorMap[tid]);
            return prepareResult;
        }

        private void Cleanup(int tid)
        {
            coordinatorMap.Remove(tid);
        }
    }
}