using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
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
        //protected Random rnd;
        private IGlobalTransactionCoordinatorGrain myCoordinator;
        private TransactionScheduler myScheduler;

        public TransactionExecutionGrain(String myUserClassName){
            
            this.myUserClassName = myUserClassName;
        }

        public async override Task OnActivateAsync()
        {
            myPrimaryKey = this.GetPrimaryKey();
            var configTuple = await this.GrainFactory.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0)).GetConfiguration(myUserClassName, myPrimaryKey);
            // configTuple: Tuple<ExecutionGrainConfiguration, uint>
            // uint: coordinator ID
            myCoordinator = this.GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(Helper.convertUInt32ToGuid(configTuple.Item2));
            // new HybridState<TState>(ConcurrencyType type)
            this.state = new HybridState<TState>(configTuple.Item1.nonDetCCConfiguration.nonDetConcurrencyManager);
            if(configTuple.Item1.logConfiguration.isLoggingEnabled)
            {
                log = new Simple2PCLoggingProtocol<TState>(this.GetType().ToString(), myPrimaryKey, configTuple.Item1.logConfiguration.loggingStorageWrapper);
            }
            batchScheduleMap = new Dictionary<int, DeterministicBatchSchedule>();
            myScheduler = new TransactionScheduler(batchScheduleMap, configTuple.Item1.maxNonDetWaitingLatencyInMs, this.GrainFactory);
            coordinatorMap = new Dictionary<int, Guid>();
        }

        /**
         * Submit a determinictic transaction to the coordinator. 
         * On receiving the returned transaction context, start the execution of a transaction.
         * 
         */
        public async Task<FunctionResult> StartTransaction(Dictionary<Guid, Tuple<String,int>> grainAccessInformation, String startFunction, FunctionInput inputs)
        {
            TransactionContext context = await myCoordinator.NewTransaction(grainAccessInformation);
            /*
            inputs.context = context;
            FunctionCall c1 = new FunctionCall(this.GetType(), startFunction, inputs);
            Task<FunctionResult> t1 = this.Execute(c1);
            Task t2 = myCoordinator.checkBatchCompletion(context.batchID);
            await Task.WhenAll(t1, t2);
            return t1.Result;*/
            return new FunctionResult();
        }
        
        public async Task<FunctionResult> StartTransaction(String startFunction, FunctionInput functionCallInput)
        {
            FunctionResult result = null;
            TransactionContext context = null;
            Task<FunctionResult> t1 = null;
            Boolean canCommit = false;
            try
            {
                context = await myCoordinator.NewTransaction();
                //if (context.transactionID == 200)
                //  Console.WriteLine($"start txn 200");
                //myScheduler.ackBatchCommit(context.highestBatchIdCommitted);
                functionCallInput.context = context;
                //context = functionCallInput.context;    // changed by Yijian (when tid is assigned by client)
                context.coordinatorKey = this.myPrimaryKey;
                //Console.WriteLine($"Transaction {context.transactionID}: is started.\n");
                FunctionCall c1 = new FunctionCall(this.GetType(), startFunction, functionCallInput);
                t1 = this.Execute(c1);
                await t1;
                //Console.WriteLine($"Transaction {context.transactionID}: completed executing.\n");
                result = new FunctionResult(t1.Result.resultObject);
                canCommit = !t1.Result.hasException();
                
                //canCommit = canCommit & serializable;
                if (t1.Result.grainsInNestedFunctions.Count > 1 && canCommit)
                {
                    canCommit = this.CheckSerializability(t1.Result).Result;
                    if (canCommit) canCommit = await Prepare_2PC(context.transactionID, myPrimaryKey, t1.Result);
                } 
                else Debug.Assert(t1.Result.grainsInNestedFunctions.ContainsKey(myPrimaryKey) || !canCommit);

                if (canCommit)
                {
                    if (t1.Result.grainsInNestedFunctions.Count == 1)
                    {
                        Debug.Assert(t1.Result.grainsInNestedFunctions.ContainsKey(this.myPrimaryKey));
                        await Commit(context.transactionID);
                    }
                    else await Commit_2PC(context.transactionID, t1.Result);
                } 
                else
                {
                    await Abort_2PC(context.transactionID, t1.Result);
                    result.setException(MyExceptionType.TwoPhaseCommit);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n Exception(StartTransaction)::{this.myPrimaryKey}: transaction {startFunction} {context.transactionID} exception {e.Message}");
            }

            if(t1.Result.beforeSet.Count != 0)
            {
                //Console.WriteLine($"Non-det txn {context.transactionID} is waiting for batch {t1.Result.maxBeforeBid} to commit.");
                if (t1.Result.grainWithHighestBeforeBid.Item1 == myPrimaryKey && t1.Result.grainWithHighestBeforeBid.Item2.Equals(this.myUserClassName))
                {
                    await this.WaitForBatchCommit(t1.Result.maxBeforeBid);
                }
                else await this.GrainFactory.GetGrain<ITransactionExecutionGrain>(t1.Result.grainWithHighestBeforeBid.Item1, t1.Result.grainWithHighestBeforeBid.Item2).WaitForBatchCommit(t1.Result.maxBeforeBid);    
            }
            //if (context.transactionID == 20000)
              //  Console.WriteLine($"txn 20000 finished");
            return result;
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
                if (log != null)
                    logTask = log.HandleBeforePrepareIn2PC(tid, coordinatorKey, participants);

                List<Task<Boolean>> prepareResult = new List<Task<Boolean>>();
                //Console.WriteLine($"Transaction {context.transactionID} send prepare messages to {grainIDsInTransaction.Count} grains. \n");
                foreach (var grain in grainIDsInTransaction)
                {
                    prepareResult.Add(this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Prepare(tid));
                }
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
        // TODO: changed by Yijian
        public async Task<Boolean> CheckSerializability(FunctionResult result)
        {
            if (result.beforeSet.Count == 0) return true;
            if (result.isBeforeAfterConsecutive && result.maxBeforeBid < result.minAfterBid) return true;
            if (result.maxBeforeBid >= result.minAfterBid) return false;
            // isBeforeAfterConsecutive = false && result.maxBeforeBid < result.minAfterBid
            //TODO HashSet<int> completeAfterSet = await myCoordinator.GetCompleteAfterSet(result.maxBeforeBidPerGrain, result.grainsInNestedFunctions);
            return false;
        }

        public async Task WaitForBatchCommit(int bid)
        {
            await myScheduler.waitForBatchCommit(bid);
        }


        public async Task Commit_2PC(int tid, FunctionResult result)
        {
            Dictionary<Guid, String> grainIDsInTransaction = result.grainsInNestedFunctions;
            List<Task> commitTasks = new List<Task>();
            if (log != null)
                commitTasks.Add(log.HandleOnCommitIn2PC(state, tid, coordinatorMap[tid]));
            //Console.WriteLine($"Transaction {context.transactionID}: prepared to commit. \n");
            foreach (var grain in grainIDsInTransaction)
            {
                commitTasks.Add(this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Commit(tid));
            }
            await Task.WhenAll(commitTasks);
            //Console.WriteLine($"Transaction {tid}: committed. \n");
        }

        public async Task Abort_2PC(int tid, FunctionResult result)
        {
            Dictionary<Guid, String> grainIDsInTransaction = result.grainsInNestedFunctions;
            List<Task> abortTasks = new List<Task>();
            //Presume Abort
            //Console.WriteLine($"Transaction {context.transactionID}: prepared to abort. \n");
            foreach (var grain in grainIDsInTransaction)
            {
                abortTasks.Add(this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Abort(tid));
            }
            await Task.WhenAll(abortTasks);
            //Console.WriteLine($"Transaction {tid}: aborted. \n");
        }

        /**
         * On receive the schedule for a specific batch
         * 1. Store this schedule.
         * 2. Check if there is function call that should be executed now, and execute it if yes.
         */
        public Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule)
        {
            /*
            //Console.WriteLine($"\n {this.myPrimaryKey}: receive bid {schedule.batchID}, last bid = {schedule.lastBatchID}, highest commit bid = {schedule.highestCommittedBatchId}");        
            // Add by Yijian (can handle the situation when receive a schedule whose lastBatchID is also -1)
            myScheduler.ackBatchCommit(schedule.highestCommittedBatchId);
            batchScheduleMap.Add(schedule.batchID, schedule);
            myScheduler.RegisterDeterministicBatchSchedule(schedule.batchID);*/
            return Task.CompletedTask;
        }

        /**
         *Allow reentrance to enforce ordered execution
         */
        public async Task<FunctionResult> Execute(FunctionCall call)
        {
            if (call.funcInput.context.isDeterministic == false)
            {
                //Non-deterministic exection
                FunctionResult invokeRet = null;
                try
                {
                    await myScheduler.waitForTurn(call.funcInput.context.transactionID);
                    invokeRet = await InvokeFunction(call);
                }
                catch(Exception e)
                {
                    Console.WriteLine($"\n Exception::InvokeFunction: {e.Message.ToString()}");
                }                

                //Update before set and after set
                int tid = call.funcInput.context.transactionID;
                updateExecutionResult(tid, invokeRet);
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

                    var coordinator = this.GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(batchScheduleMap[bid].globalCoordinator);
                    Task ack = coordinator.AckBatchCompletion(bid, myPrimaryKey);
                }
                return ret;
                //XXX: Check if this works -> return new FunctionResult(ret);
            }
        }

        //Update the metadata of the execution results, including accessed grains, before/after set, etc.
        public void updateExecutionResult(int tid, FunctionResult invokeRet)
        {
            if (invokeRet.grainWithHighestBeforeBid == null)
                invokeRet.grainWithHighestBeforeBid = new Tuple<Guid, string>(this.myPrimaryKey, this.myUserClassName);

            int maxBeforeBid, minAfterBid;
            bool isBeforeAfterConsecutive = false;

            if (!invokeRet.grainsInNestedFunctions.ContainsKey(this.myPrimaryKey))
                invokeRet.grainsInNestedFunctions.Add(myPrimaryKey, myUserClassName);
                
            var beforeSet = myScheduler.getBeforeSet(tid, out maxBeforeBid);
            var afterSet = myScheduler.getAfterSet(tid, maxBeforeBid, out minAfterBid);
            invokeRet.beforeSet.UnionWith(beforeSet);
            invokeRet.afterSet.UnionWith(afterSet);
            // chenged by Yijian
            if (minAfterBid == int.MaxValue) isBeforeAfterConsecutive = false;
            else if (batchScheduleMap[minAfterBid].lastBatchID == maxBeforeBid || maxBeforeBid == int.MinValue)
                isBeforeAfterConsecutive = true;
            invokeRet.setSchedulingStatistics(maxBeforeBid, minAfterBid, isBeforeAfterConsecutive, new Tuple<Guid, string>(this.myPrimaryKey, this.myUserClassName));
        }

        public async Task<FunctionResult> InvokeFunction(FunctionCall call)
        {
            var context = call.funcInput.context;
            var key = (context.isDeterministic) ? context.batchID : context.transactionID;            
            if(!coordinatorMap.ContainsKey(key)) coordinatorMap.Add(key, context.coordinatorKey);
            FunctionInput functionCallInput = call.funcInput;                        
            MethodInfo mi = call.type.GetMethod(call.func);
            Task<FunctionResult> t = (Task<FunctionResult>)mi.Invoke(this, new object[] { functionCallInput });
            await t;
            return t.Result;
        }

        private void Cleanup(int tid)
        {
            coordinatorMap.Remove(tid);
        }

        public async Task Abort(int tid)
        {
            //Console.WriteLine($"\n\n Grain {this.myPrimaryKey}: receives Abort message for transaction {tid}. \n\n");
            if (state == null) return;

            var tasks = new List<Task>();
            tasks.Add(this.state.Abort(tid));

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
            tasks.Add(this.state.Commit(tid));
            if (log != null) tasks.Add(log.HandleOnCommitIn2PC(state, tid, coordinatorMap[tid]));
            myScheduler.ackComplete((int)tid);
            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        /**
         * Stateless grain always vote "yes" for 2PC.
         */
        public async Task<bool> Prepare(int tid)
        {
            if (state == null) return true;
            var prepareResult = await this.state.Prepare(tid);
            if (prepareResult && log != null)
            if (log != null) await log.HandleOnPrepareIn2PC(state, tid, coordinatorMap[tid]);
            return prepareResult;
        }
    }
}
