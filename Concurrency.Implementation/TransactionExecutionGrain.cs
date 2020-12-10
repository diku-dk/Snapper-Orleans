using System;
using Orleans;
using Utilities;
using System.Diagnostics;
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
        public int intraCount = 0;
        public int interCount = 0;

        public int myID;
        private int coordID;
        public int numCoord;
        protected string grainClassName;
        private int highestCommittedBid;
        private TransactionScheduler myScheduler;
        protected ITransactionalState<TState> state;
        private Dictionary<int, int> coordinatorMap;    // <act tid, grainID who starts the act>
        protected ILoggingProtocol<TState> log = null;
        private SortedDictionary<int, DateTime> receiveBatchTime;
        private IGlobalTransactionCoordinatorGrain myCoordinator;
        private Dictionary<int, TaskCompletionSource<bool>> batchCommit;
        private TimeSpan deadlockTimeout = TimeSpan.FromMilliseconds(20);
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        private Dictionary<int, IGlobalTransactionCoordinatorGrain> coordList;  // <coordID, coord>

        public TransactionExecutionGrain(string grainClassName)
        {
            this.grainClassName = grainClassName;
        }

        public async override Task OnActivateAsync()
        {
            myID = (int)this.GetPrimaryKeyLong();
            var configTuple = await GrainFactory.GetGrain<IConfigurationManagerGrain>(0).GetConfiguration(myID);
            // <ExecutionGrainConfiguration, coordinator ID>
            coordID = configTuple.Item2;
            numCoord = configTuple.Item3;
            myCoordinator = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(coordID);
            state = new HybridState<TState>(configTuple.Item1.nonDetCCConfiguration.nonDetConcurrencyManager);
            var logConfig = configTuple.Item1.logConfiguration;
            if (logConfig.isLoggingEnabled) log = new Simple2PCLoggingProtocol<TState>(GetType().ToString(), myID, logConfig.dataFormat, logConfig.loggingStorageWrapper);

            highestCommittedBid = -1;
            coordinatorMap = new Dictionary<int, int>();
            receiveBatchTime = new SortedDictionary<int, DateTime>();
            batchScheduleMap = new Dictionary<int, DeterministicBatchSchedule>();
            myScheduler = new TransactionScheduler(batchScheduleMap, myID);
            batchCommit = new Dictionary<int, TaskCompletionSource<bool>>();
            coordList = new Dictionary<int, IGlobalTransactionCoordinatorGrain>();
            coordList.Add(coordID, myCoordinator);
        }

        public async Task PrintData()
        {
            if (batchCommit.Count > 0) Console.WriteLine($"grain {myID} batchCommit {batchCommit.Count}");
            if (receiveBatchTime.Count > 0) Console.WriteLine($"grain {myID} receiveBatchTime {receiveBatchTime.Count}");
            if (batchScheduleMap.Count > 0) Console.WriteLine($"grain {myID} batchScheduleMap {batchScheduleMap.Count}");
            if (coordinatorMap.Count > 0) Console.WriteLine($"grain {myID} coordinatorMap {coordinatorMap.Count}");

            var map = myScheduler.inBatchTransactionCompletionMap;
            if (map.Count > 0) Console.WriteLine($"grain {myID} myScheduler.inBatchTransactionCompletionMap {map.Count}");

            var info = myScheduler.scheduleInfo;
            if (info.nodes.Count > 0) Console.WriteLine($"grain {myID} scheduleInfo.nodes {info.nodes.Count}");
            if (info.nonDetTxnToScheduleMap.Count > 0) Console.WriteLine($"grain {myID} scheduleInfo.nonDetTxnToScheduleMap {info.nonDetTxnToScheduleMap.Count}");
            if (info.nonDetBatchScheduleMap.Count > 0) Console.WriteLine($"grain {myID} scheduleInfo.nonDetBatchScheduleMap {info.nonDetBatchScheduleMap.Count}");
        }

        public async Task<Tuple<int, int>> GetSetCount()
        {
            var res = new Tuple<int, int>(intraCount, interCount);
            intraCount = 0;
            interCount = 0;
            await Task.CompletedTask;
            return res;
        }

        /**
         * Submit a determinictic transaction to the coordinator. 
         * On receiving the returned transaction context, start the execution of a transaction.
         * 
         */
        public async Task<TransactionResult> StartTransaction(Dictionary<int, int> grainAccessInformation, string startFunction, FunctionInput inputs)
        {
            if (Constants.multiSilo)
            {
                var intraSilo = Helper.intraSilo(numCoord, myID, false, coordID, true);
                if (intraSilo) intraCount++;
                else interCount++;
            }
            else intraCount++;

            var arrive = DateTime.Now;
            var context = await myCoordinator.NewTransaction(grainAccessInformation);
            var emit = DateTime.Now;
            if (highestCommittedBid < context.highestBatchIdCommitted) highestCommittedBid = context.highestBatchIdCommitted;
            inputs.context = context;
            var c1 = new FunctionCall(GetType(), startFunction, inputs);
            var t1 = Execute(c1);
            await t1;
            var finish = DateTime.Now;
            var getBatch = receiveBatchTime[context.batchID];
            if (highestCommittedBid < context.batchID)
            {
                Debug.Assert(batchCommit.ContainsKey(context.batchID));
                await batchCommit[context.batchID].Task;
            }
            var commit = DateTime.Now;
            var res = new TransactionResult(false, t1.Result.resultObject);  // PACT never abort
            res.arriveTime = arrive;
            res.emitTime = emit;
            res.finishTime = finish;
            res.commitTime = commit;
            res.batchTime = getBatch;
            return res;
        }

        public async Task<TransactionResult> StartTransaction(string startFunction, FunctionInput functionCallInput)
        {
            var arrive = DateTime.Now;
            var emit = DateTime.Now;
            DateTime finish;
            DateTime commit;
            TransactionContext context = null;
            Task<FunctionResult> t1 = null;
            var canCommit = true;
            var res = new TransactionResult();
            try
            {
                if (Constants.multiSilo)
                {
                    var intraSilo = Helper.intraSilo(numCoord, myID, false, coordID, true);
                    if (intraSilo) intraCount++;
                    else interCount++;
                }
                else intraCount++;

                context = await myCoordinator.NewTransaction();
                emit = DateTime.Now;
                if (highestCommittedBid < context.highestBatchIdCommitted) highestCommittedBid = context.highestBatchIdCommitted;
                functionCallInput.context = context;
                context.coordinatorKey = myID;
                var c1 = new FunctionCall(GetType(), startFunction, functionCallInput);
                t1 = Execute(c1);
                await t1;
                canCommit = !t1.Result.hasException();
                Debug.Assert(t1.Result.grainsInNestedFunctions.Contains(myID));
                if (canCommit)
                {
                    canCommit = CheckSerializability(t1.Result);
                    if (canCommit) canCommit = await Prepare_2PC(context.transactionID, myID, t1.Result);
                    else res.Exp_Serializable = true;
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
            finish = DateTime.Now;
            if (canCommit && t1.Result.beforeSet.Count != 0 && highestCommittedBid < t1.Result.maxBeforeBid)
            {
                var grainID = t1.Result.grainWithHighestBeforeBid;
                if (grainID == myID) await WaitForBatchCommit(t1.Result.maxBeforeBid);
                else
                {
                    var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID, grainClassName);
                    var new_bid = await grain.WaitForBatchCommit(t1.Result.maxBeforeBid);
                    if (highestCommittedBid < new_bid) highestCommittedBid = new_bid;

                    if (Constants.multiSilo)
                    {
                        var intraSilo = Helper.intraSilo(numCoord, myID, false, grainID, false);
                        if (intraSilo) intraCount++;
                        else interCount++;
                    }
                    else intraCount++;
                }
            }
            commit = DateTime.Now;
            res.arriveTime = arrive;
            res.emitTime = emit;
            res.finishTime = finish;
            res.commitTime = commit;
            res.isDet = false;
            return res;
        }

        /**
         * On receive the schedule for a specific batch
         * 1. Store this schedule.
         * 2. Check if there is function call that should be executed now, and execute it if yes.
         */
        public Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule)
        {
            receiveBatchTime.Add(schedule.batchID, DateTime.Now);
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

            var removeList = new List<int>();
            foreach (var batch in receiveBatchTime)
            {
                if (batch.Key < bid) removeList.Add(batch.Key);
            }
            foreach (var batch in removeList) receiveBatchTime.Remove(batch);
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
            if (!invokeRet.grainsInNestedFunctions.Contains(myID)) invokeRet.grainsInNestedFunctions.Add(myID);
        }

        //Update the metadata of the execution results, including accessed grains, before/after set, etc.
        private void updateExecutionResult(int tid, FunctionResult invokeRet)
        {
            if (invokeRet.grainWithHighestBeforeBid == -1) invokeRet.grainWithHighestBeforeBid = myID;

            int maxBeforeBid, minAfterBid;
            bool isBeforeAfterConsecutive;

            if (!invokeRet.grainsInNestedFunctions.Contains(myID)) invokeRet.grainsInNestedFunctions.Add(myID);

            var beforeSet = myScheduler.getBeforeSet(tid, out maxBeforeBid);
            var afterSet = myScheduler.getAfterSet(maxBeforeBid, out minAfterBid);
            invokeRet.beforeSet.UnionWith(beforeSet);
            invokeRet.afterSet.UnionWith(afterSet);
            if (minAfterBid == int.MaxValue) isBeforeAfterConsecutive = false;
            else if (maxBeforeBid == int.MinValue) isBeforeAfterConsecutive = true;
            else if (batchScheduleMap.ContainsKey(minAfterBid) && batchScheduleMap[minAfterBid].lastBatchID == maxBeforeBid) isBeforeAfterConsecutive = true;
            else isBeforeAfterConsecutive = false;
            invokeRet.setSchedulingStatistics(maxBeforeBid, minAfterBid, isBeforeAfterConsecutive, myID);
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

        public bool CheckSerializability(FunctionResult result)
        {
            if (result.beforeSet.Count == 0) return true;
            if (result.maxBeforeBid <= highestCommittedBid) return true;
            if (result.isBeforeAfterConsecutive && result.maxBeforeBid < result.minAfterBid) return true;
            if (result.maxBeforeBid >= result.minAfterBid) return false;
            return false;
        }

        private async Task<bool> Prepare_2PC(int tid, int coordinatorKey, FunctionResult result)
        {
            var grainIDsInTransaction = result.grainsInNestedFunctions;
            var hasException = result.hasException();
            var canCommit = !hasException;
            if (!hasException)
            {
                var logTask = Task.CompletedTask;
                if (log != null) logTask = log.HandleBeforePrepareIn2PC(tid, coordinatorKey, grainIDsInTransaction);

                var prepareResult = new List<Task<bool>>();
                foreach (var grain in grainIDsInTransaction)
                {
                    if (grain == myID) prepareResult.Add(Prepare(tid));
                    else
                    {
                        prepareResult.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(grain, grainClassName).Prepare(tid));
                        if (Constants.multiSilo)
                        {
                            var intraSilo = Helper.intraSilo(numCoord, myID, false, grain, false);
                            if (intraSilo) intraCount += 2;
                            else interCount += 2;
                        }
                        else intraCount += 2;
                    }
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
            var grainIDsInTransaction = result.grainsInNestedFunctions;
            var commitTasks = new List<Task>();
            if (log != null) commitTasks.Add(log.HandleOnCommitIn2PC(state, tid, coordinatorMap[tid]));
            foreach (var grain in grainIDsInTransaction)
            {
                if (grain == myID) commitTasks.Add(Commit(tid));
                else commitTasks.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(grain, grainClassName).Commit(tid));
            }
            await Task.WhenAll(commitTasks);
        }

        private async Task Abort_2PC(int tid, FunctionResult result)
        {
            var grainIDsInTransaction = result.grainsInNestedFunctions;
            var abortTasks = new List<Task>();
            //Presume Abort
            foreach (var grain in grainIDsInTransaction)
            {
                if (grain == myID) abortTasks.Add(Abort(tid));
                else abortTasks.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(grain, grainClassName).Abort(tid));
            }
            await Task.WhenAll(abortTasks);
        }

        public async Task Abort(int tid)
        {
            if (state == null) return;
            var tasks = new List<Task>();
            tasks.Add(state.Abort(tid));

            //Presume Abort
            //if (log != null)
            //tasks.Add(log.HandleOnAbortIn2PC(state, tid, coordinatorMap[tid]));
            myScheduler.ackComplete((int)tid);
            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        public async Task Commit(int tid)
        {
            if (state == null) return;
            var tasks = new List<Task>();
            tasks.Add(state.Commit(tid));
            if (log != null) tasks.Add(log.HandleOnCommitIn2PC(state, tid, coordinatorMap[tid]));
            myScheduler.ackComplete(tid);
            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        public async Task<bool> Prepare(int tid)
        {
            if (state == null) return true;  // Stateless grain always vote "yes" for 2PC
            var prepareResult = await state.Prepare(tid);
            if (prepareResult && log != null) await log.HandleOnPrepareIn2PC(state, tid, coordinatorMap[tid]);
            return prepareResult;
        }

        private void Cleanup(int tid)
        {
            coordinatorMap.Remove(tid);
        }
    }
}