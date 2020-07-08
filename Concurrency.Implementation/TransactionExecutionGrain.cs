using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Concurrency.Interface;
using Concurrency.Interface.Logging;
using Utilities;
using System.Diagnostics;
using Concurrency.Implementation.Logging;
using Orleans;

namespace Concurrency.Implementation
{
    public abstract class TransactionExecutionGrain<TState> : Grain, ITransactionExecutionGrain where TState : ICloneable, new()
    {
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        private Dictionary<int, Guid> coordinatorMap;

        protected Guid myPrimaryKey;
        protected ITransactionalState<TState> state;
        protected ILoggingProtocol<TState> log = null;
        protected String myUserClassName;
        private IGlobalTransactionCoordinatorGrain myCoordinator;
        private TransactionScheduler myScheduler;
        private int highestCommittedBid;

        private int lastCommitTid = -1;

        public TransactionExecutionGrain(String myUserClassName)
        {
            this.myUserClassName = myUserClassName;
        }

        public async override Task OnActivateAsync()
        {
            highestCommittedBid = -1;
            myPrimaryKey = this.GetPrimaryKey();
            var configTuple = await GrainFactory.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0)).GetConfiguration(myUserClassName, myPrimaryKey);
            // configTuple: Tuple<ExecutionGrainConfiguration, uint>
            // uint: coordinator ID
            myCoordinator = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(Helper.convertUInt32ToGuid(configTuple.Item2));
            // new HybridState<TState>(ConcurrencyType type)
            state = new HybridState<TState>(configTuple.Item1.nonDetCCConfiguration.nonDetConcurrencyManager);
            if (configTuple.Item1.logConfiguration.isLoggingEnabled)
            {
                log = new Simple2PCLoggingProtocol<TState>(GetType().ToString(), myPrimaryKey, configTuple.Item1.logConfiguration.loggingStorageWrapper);
            }
            batchScheduleMap = new Dictionary<int, DeterministicBatchSchedule>();
            myScheduler = new TransactionScheduler(batchScheduleMap, configTuple.Item1.maxNonDetWaitingLatencyInMs, GrainFactory);
            coordinatorMap = new Dictionary<int, Guid>();
        }

        /**
         * Submit a determinictic transaction to the coordinator. 
         * On receiving the returned transaction context, start the execution of a transaction.
         * 
         */
        public async Task<FunctionResult> StartTransaction(Dictionary<Guid, Tuple<String, int>> grainAccessInformation, String startFunction, FunctionInput inputs)
        {
            TransactionContext context = await myCoordinator.NewTransaction(grainAccessInformation);
            highestCommittedBid = context.highestBatchIdCommitted;
            inputs.context = context;
            FunctionCall c1 = new FunctionCall(GetType(), startFunction, inputs);
            Task<FunctionResult> t1 = Execute(c1);
            Task t2 = myCoordinator.checkBatchCompletion(context.batchID);
            await Task.WhenAll(t1, t2);
            t1.Result.isDet = true;
            t1.Result.txnType = startFunction;
            return t1.Result;
        }

        public async Task<FunctionResult> StartTransaction(String startFunction, FunctionInput functionCallInput)
        {
            TransactionContext context = null;
            Task<FunctionResult> t1 = null;
            Boolean canCommit = false;
            try
            {
                //context = functionCallInput.context;   // added by Yijian for CC test
                context = await myCoordinator.NewTransaction();
                highestCommittedBid = context.highestBatchIdCommitted;
                functionCallInput.context = context;
                context.coordinatorKey = myPrimaryKey;
                var c1 = new FunctionCall(GetType(), startFunction, functionCallInput);
                t1 = Execute(c1);
                await t1;
                t1.Result.tid = context.transactionID;

                canCommit = !t1.Result.hasException();
                Debug.Assert(t1.Result.grainsInNestedFunctions.ContainsKey(myPrimaryKey));
                if (canCommit)
                {
                    canCommit = CheckSerializability(t1.Result).Result;
                    if (canCommit)
                    {
                        canCommit = await Prepare_2PC(context.transactionID, myPrimaryKey, t1.Result);
                        if (!canCommit) t1.Result.Exp_2PC = true;
                    }
                    else t1.Result.Exp_NotSerializable = true;
                }

                if (canCommit)
                {
                    /*
                    if (!t1.Result.readOnly)  // all RW transactions commit in timestamp order
                    {
                        Debug.Assert(context.transactionID > lastCommitTid);
                        lastCommitTid = context.transactionID;
                    } */
                    await Commit_2PC(context.transactionID, t1.Result);
                } 
                else
                {
                    t1.Result.setException();
                    await Abort_2PC(context.transactionID, t1.Result);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n Exception(StartTransaction)::{myPrimaryKey}: transaction {startFunction} {context.transactionID} exception {e.Message}, {e.StackTrace}");
            }

            if (t1.Result.beforeSet.Count != 0)
            {
                if (t1.Result.grainWithHighestBeforeBid.Item1 == myPrimaryKey && t1.Result.grainWithHighestBeforeBid.Item2.Equals(myUserClassName))
                    await WaitForBatchCommit(t1.Result.maxBeforeBid);
                else await GrainFactory.GetGrain<ITransactionExecutionGrain>(t1.Result.grainWithHighestBeforeBid.Item1, t1.Result.grainWithHighestBeforeBid.Item2).WaitForBatchCommit(t1.Result.maxBeforeBid);
            }
            t1.Result.isDet = false;
            t1.Result.txnType = startFunction;
            return t1.Result;
        }

        /**
         * On receive the schedule for a specific batch
         * 1. Store this schedule.
         * 2. Check if there is function call that should be executed now, and execute it if yes.
         */
        public Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule)
        {
            //Console.WriteLine($"{myPrimaryKey} receives batch {schedule.batchID}. ");
            // Add by Yijian (can handle the situation when receive a schedule whose lastBatchID is also -1)
            myScheduler.ackBatchCommit(schedule.highestCommittedBatchId);
            batchScheduleMap.Add(schedule.batchID, schedule);
            myScheduler.RegisterDeterministicBatchSchedule(schedule.batchID);
            return Task.CompletedTask;
        }

        /**
         *Allow reentrance to enforce ordered execution
         */
        public async Task<FunctionResult> Execute(FunctionCall call)
        {
            if (call.funcInput.context.isDeterministic == false)
            {
                FunctionResult invokeRet = null;
                try
                {
                    await myScheduler.waitForTurn(call.funcInput.context.transactionID);
                    invokeRet = await InvokeFunction(call);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"\n Exception::InvokeFunction: {e.Message}");
                }
                int tid = call.funcInput.context.transactionID;
                updateExecutionResult(tid, invokeRet);   //Update before set and after set
                return invokeRet;
            }
            else
            {
                int tid = call.funcInput.context.transactionID;
                int bid = call.funcInput.context.batchID;
                var myTurnIndex = await myScheduler.waitForTurn(bid, tid);

                //Execute the function call;
                var ret = await InvokeFunction(call);
                if (myScheduler.ackComplete(bid, tid, myTurnIndex))
                {
                    //The scheduler has switched batches, need to commit now
                    if (log != null && state != null)
                        await log.HandleOnCompleteInDeterministicProtocol(state, bid, batchScheduleMap[bid].globalCoordinator);

                    var coordinator = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(batchScheduleMap[bid].globalCoordinator);
                    var ack = coordinator.AckBatchCompletion(bid, myPrimaryKey);
                }
                return ret;
            }
        }

        //Update the metadata of the execution results, including accessed grains, before/after set, etc.
        public void updateExecutionResult(int tid, FunctionResult invokeRet)
        {
            if (invokeRet.grainWithHighestBeforeBid == null)
                invokeRet.grainWithHighestBeforeBid = new Tuple<Guid, string>(myPrimaryKey, myUserClassName);

            int maxBeforeBid, minAfterBid;
            bool isBeforeAfterConsecutive = false;

            if (!invokeRet.grainsInNestedFunctions.ContainsKey(myPrimaryKey))
                invokeRet.grainsInNestedFunctions.Add(myPrimaryKey, myUserClassName);

            var beforeSet = myScheduler.getBeforeSet(tid, out maxBeforeBid);
            var afterSet = myScheduler.getAfterSet(tid, maxBeforeBid, out minAfterBid);
            invokeRet.beforeSet.UnionWith(beforeSet);
            invokeRet.afterSet.UnionWith(afterSet);
            // chenged by Yijian
            if (minAfterBid == int.MaxValue) isBeforeAfterConsecutive = false;
            else if (batchScheduleMap[minAfterBid].lastBatchID == maxBeforeBid || maxBeforeBid == int.MinValue)
                isBeforeAfterConsecutive = true;
            invokeRet.setSchedulingStatistics(maxBeforeBid, minAfterBid, isBeforeAfterConsecutive, new Tuple<Guid, string>(myPrimaryKey, myUserClassName));
            if (invokeRet.highestCommittedBid < highestCommittedBid) invokeRet.highestCommittedBid = highestCommittedBid;
        }

        public async Task<FunctionResult> InvokeFunction(FunctionCall call)
        {
            var context = call.funcInput.context;
            var key = (context.isDeterministic) ? context.batchID : context.transactionID;
            if (!coordinatorMap.ContainsKey(key)) coordinatorMap.Add(key, context.coordinatorKey);
            FunctionInput functionCallInput = call.funcInput;
            MethodInfo mi = call.type.GetMethod(call.func);
            Task<FunctionResult> t = (Task<FunctionResult>)mi.Invoke(this, new object[] { functionCallInput });
            await t;
            return t.Result;
        }

        public async Task<Boolean> CheckSerializability(FunctionResult result)
        {
            await Task.CompletedTask;
            if (result.beforeSet.Count == 0) return true;
            if (result.maxBeforeBid <= highestCommittedBid) return true;
            if (result.maxBeforeBid <= result.highestCommittedBid) return true;
            if (result.isBeforeAfterConsecutive && result.maxBeforeBid < result.minAfterBid) return true;
            if (result.maxBeforeBid >= result.minAfterBid) return false;
            return false;
        }

        public async Task WaitForBatchCommit(int bid)
        {
            await myScheduler.waitForBatchCommit(bid);
        }

        public async Task<Boolean> Prepare_2PC(int tid, Guid coordinatorKey, FunctionResult result)
        {
            Dictionary<Guid, String> grainIDsInTransaction = result.grainsInNestedFunctions;
            bool hasException = result.hasException();
            bool canCommit = !hasException;
            if (!hasException)
            {
                // Prepare Phase
                HashSet<Guid> participants = new HashSet<Guid>();
                participants.UnionWith(grainIDsInTransaction.Keys);
                Task logTask = Task.CompletedTask;
                if (log != null) logTask = log.HandleBeforePrepareIn2PC(tid, coordinatorKey, participants);

                List<Task<Boolean>> prepareResult = new List<Task<Boolean>>();
                foreach (var grain in grainIDsInTransaction)
                    prepareResult.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Prepare(tid));
                
                await Task.WhenAll(logTask, Task.WhenAll(prepareResult));
                foreach (Task<Boolean> vote in prepareResult)
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
        
        public async Task Commit_2PC(int tid, FunctionResult result)
        {
            var grainIDsInTransaction = result.grainsInNestedFunctions;
            var commitTasks = new List<Task<Tuple<Guid, Object>>>();
            if (log != null) await log.HandleOnCommitIn2PC(state, tid, coordinatorMap[tid]);
            foreach (var grain in grainIDsInTransaction)
                commitTasks.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Commit(tid));
            await Task.WhenAll(commitTasks);
            foreach (var t in commitTasks) result.afterState.Add(t.Result.Item1, t.Result.Item2); // added by Yijian
        }

        public async Task Abort_2PC(int tid, FunctionResult result)
        {
            var grainIDsInTransaction = result.grainsInNestedFunctions;
            var abortTasks = new List<Task>();
            //Presume Abort
            foreach (var grain in grainIDsInTransaction)
                abortTasks.Add(GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Abort(tid));
            await Task.WhenAll(abortTasks);
        }

        public async Task Abort(int tid)
        {
            //Console.WriteLine($"{myPrimaryKey} aborts transaction {tid}. ");
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

        public async Task<Tuple<Guid, Object>> Commit(int tid)
        {
            if (state == null) return null;
            var tasks = new List<Task>();
            await state.Commit(tid);
            var res = new Tuple<Guid, object>(myPrimaryKey, state.GetCommittedState(tid).Clone());
            //tasks.Add(state.Commit(tid));
            if (log != null) tasks.Add(log.HandleOnCommitIn2PC(state, tid, coordinatorMap[tid]));
            myScheduler.ackComplete((int)tid);
            Cleanup(tid);
            await Task.WhenAll(tasks);
            return res;   // added by Yijian
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
