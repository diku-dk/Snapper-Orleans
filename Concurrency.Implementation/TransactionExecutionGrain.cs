using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using Concurrency.Interface.Logging;
using Concurrency.Utilities;
using Concurrency.Implementation.Logging;
using Orleans;
using Utilities;

namespace Concurrency.Implementation
{
    public abstract class TransactionExecutionGrain<TState> : Grain, ITransactionExecutionGrain
    {
        public INondeterministicTransactionCoordinator ndtc;
        public IDeterministicTransactionCoordinator dtc;
        private int lastBatchId;
        private Dictionary<int, BatchSchedule> batchScheduleMap;
        private Dictionary<long, Guid> coordinatorMap;
        private Queue<BatchSchedule> batchQueue;
        private Dictionary<int, List<TaskCompletionSource<Boolean>>> promiseMap;
        protected Guid myPrimaryKey;
        protected ITransactionalState<TState> state;
        protected ILoggingProtocol<TState> log = null;
        protected String myUserClassName;

        public TransactionExecutionGrain(ITransactionalState<TState> state){
            this.state = state;
        }

        public TransactionExecutionGrain()
        {

        }
        public override Task OnActivateAsync()
        {

            ndtc = this.GrainFactory.GetGrain<INondeterministicTransactionCoordinator>(Helper.convertUInt32ToGuid(0));
            //dtc = this.GrainFactory.GetGrain<IDeterministicTransactionCoordinator>(Helper.convertUInt32ToGuid(0));
            batchQueue = new Queue<BatchSchedule>();
            batchScheduleMap = new Dictionary<int, BatchSchedule>();

            //Initialize the "batch" with id "-1" as completed;
            lastBatchId = -1;
            batchScheduleMap.Add(lastBatchId, new BatchSchedule(lastBatchId, true));

            promiseMap = new Dictionary<int, List<TaskCompletionSource<Boolean>>>();
            myPrimaryKey = this.GetPrimaryKey();
            //Enable the following line for logging
            //log = new Simple2PCLoggingProtocol<TState>(myPrimaryKey);
            coordinatorMap = new Dictionary<long, Guid>();
            return base.OnActivateAsync();
        }


        /**
         * Submit a determinictic transaction to the coordinator. 
         * On receiving the returned transaction context, start the execution of a transaction.
         * 
         */

        public async Task<FunctionResult> StartTransaction(Dictionary<Guid, Tuple<String,int>> grainAccessInformation, String startFunction, FunctionInput inputs)
        {
            TransactionContext context = await dtc.NewTransaction(grainAccessInformation);
            inputs.context = context;
            FunctionCall c1 = new FunctionCall(this.GetType(), startFunction, inputs);
            
            Task<FunctionResult> t1 = this.Execute(c1);
            Task t2 = dtc.checkBatchCompletion(context);
            await Task.WhenAll(t1, t2);
            //Console.WriteLine($"Transaction {context.transactionID}: completed executing.\n");
            return t1.Result;

        }

        public async Task<FunctionResult> StartTransaction(String startFunction, FunctionInput functionCallInput)
        {

            TransactionContext context = await ndtc.NewTransaction();
            functionCallInput.context = context;
            context.coordinatorKey = this.myPrimaryKey;
            //Console.WriteLine($"Transaction {context.transactionID}: is started.\n");
            FunctionCall c1 = new FunctionCall(this.GetType(), startFunction, functionCallInput);
            Task<FunctionResult> t1 = this.Execute(c1);
            await t1;
            //Console.WriteLine($"Transaction {context.transactionID}: completed executing.\n");

            Dictionary<Guid, String> grainIDsInTransaction = t1.Result.grainsInNestedFunctions;

            FunctionResult result = new FunctionResult(t1.Result.resultObject);            
            bool hasException = t1.Result.hasException();
            bool canCommit = !hasException;
            if (!hasException)
            {
                // Prepare Phase
                HashSet<Guid> participants = new HashSet<Guid>();
                participants.UnionWith(grainIDsInTransaction.Keys);
                Task logTask = log.HandleBeforePrepareIn2PC(context.transactionID, context.coordinatorKey, participants);

                List<Task<Boolean>> prepareResult = new List<Task<Boolean>>();
                foreach (var grain in grainIDsInTransaction)
                {
                    prepareResult.Add(this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Prepare(context.transactionID));
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

            // Commit / Abort Phase
            List<Task> commitTasks = new List<Task>();
            List<Task> abortTasks = new List<Task>();
            if (canCommit)
            {
                if(log != null)
                    commitTasks.Add(log.HandleOnCommitIn2PC(state, context.transactionID, coordinatorMap[context.transactionID]));
                //Console.WriteLine($"Transaction {context.transactionID}: prepared to commit. \n");
                foreach (var grain in grainIDsInTransaction)
                {
                    commitTasks.Add(this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Commit(context.transactionID));
                }

                await Task.WhenAll(commitTasks);
                //Console.WriteLine($"Transaction {context.transactionID}: committed. \n");
            }
            else
            {   
                //Presume Abort
                //if(log != null)
                    //abortTasks.Add(log.HandleOnAbortIn2PC(state, context.transactionID, coordinatorMap[context.transactionID]));

                //Console.WriteLine($"Transaction {context.transactionID}: prepared to abort. \n");
                foreach (var grain in grainIDsInTransaction)
                {
                    abortTasks.Add(this.GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grain.Value).Abort(context.transactionID));
                }
                await Task.WhenAll(abortTasks);
                //Console.WriteLine($"Transaction {context.transactionID}: aborted. \n");
                //Ensure the exception is set if the voting phase decides to abort
                result.setException();
            }         
            return result;
        }

        /**
         * On receive the schedule for a specific batch
         * 1. Store this schedule.
         * 2. Check if there is function call that should be executed now, and execute it if yes.
         *
         */
         
        public Task ReceiveBatchSchedule(BatchSchedule schedule)
        {
            //Console.WriteLine($"\n\n{this.GetType()}: Received schedule for batch {schedule.batchID}.\n\n");
            BatchSchedule curSchedule = new BatchSchedule(schedule);
            batchScheduleMap.Add(schedule.batchID, curSchedule);
            batchQueue.Enqueue(curSchedule);

            //Check if this batch is at the head of the queue
            BatchSchedule headSchedule = batchQueue.Peek();
            if (headSchedule.batchID == schedule.batchID)
            {
                //Check if there is a buffered function call in this batch which can be executed;
                int tid = schedule.curExecTransaction();
                if (promiseMap.ContainsKey(tid) && promiseMap[tid].Count != 0)
                {
                    promiseMap[tid][0].SetResult(true);
                }
            }

            return Task.CompletedTask;
        }

        /**
         *Allow reentrance to enforce ordered execution
         */
        public async Task<FunctionResult> Execute(FunctionCall call)
        {
            //Non-deterministic exection
            if (call.funcInput.context.isDeterministic == false)
            {
                FunctionResult invokeRet = await InvokeFunction(call);
                invokeRet.grainsInNestedFunctions.Add(myPrimaryKey, myUserClassName);
                return invokeRet;
            }

            int tid = call.funcInput.context.transactionID;
            int bid = call.funcInput.context.batchID;
            int nextTid;
            

            if (promiseMap.ContainsKey(tid) == false)
            {
                promiseMap.Add(tid, new List<TaskCompletionSource<Boolean>>());
            }
            var promise = new TaskCompletionSource<Boolean>();
            promiseMap[tid].Add(promise);

            //int nextTid;
            ////Check if this call can be executed;
            if (batchScheduleMap.ContainsKey(bid))
            {

                BatchSchedule schedule = batchQueue.Peek();
                if (schedule.batchID == bid)
                {
                    nextTid = batchScheduleMap[bid].curExecTransaction();
                    if (tid == nextTid)
                    {
                        //Console.WriteLine($"\n\n{this.GetType()}: Set Promise for Tx: {tid} in batch {bid} within Execute(). \n\n");
                        promise.SetResult(true);
                    }
                }
            }

            Object ret;

            if (promise.Task.IsCompleted)
            {
                ret = await InvokeFunction(call);
            }
            else
            {
                await promise.Task;
                ret = await InvokeFunction(call);
            }

            //Console.WriteLine($"\n\n{this.GetType()}:  Tx {tid} is executed... trying next Tx ... \n\n");
            
            //Record the execution in batch schedule
            batchScheduleMap[bid].AccessIncrement(tid);
            //Find the next transaction to be executed in this batch;
            nextTid = batchScheduleMap[bid].curExecTransaction();
            //Console.WriteLine($"\n\n{this.GetType()}: nextTid is {nextTid} \n\n");
            if (nextTid != -1)
            {
                if (promiseMap.ContainsKey(nextTid))
                {
                    //Console.WriteLine($"\n\n{this.GetType()}: Set promise result for Tx {nextTid} \n\n");
                    promiseMap[nextTid][0].SetResult(true);
                }
            }
            else
            {
                batchQueue.Dequeue();
                batchScheduleMap.Remove(bid);
                promiseMap.Remove(bid);
                //Log the state now
                if (log != null && state != null)
                    await log.HandleOnCompleteInDeterministicProtocol(state, bid, coordinatorMap[bid]);
           
                Cleanup(bid);
                //TODO: remove state of the completed batch?
                Task ack = dtc.AckBatchCompletion(bid, this.myPrimaryKey);

                //The schedule for this batch {$bid} has been completely executed. Check if any promise for next batch can be set.
                if (batchQueue.Count != 0)
                {
                    BatchSchedule nextSchedule = batchQueue.Peek();
                    nextTid = nextSchedule.curExecTransaction();
                    if (promiseMap.ContainsKey(nextTid) && promiseMap[nextTid].Count != 0)
                    {
                        promiseMap[nextTid][0].SetResult(true);
                    }
                }

            }

            return new FunctionResult(ret);            
        }

        public async Task<FunctionResult> InvokeFunction(FunctionCall call)
        {
            var context = call.funcInput.context;
            var key = (context.isDeterministic) ? context.batchID : context.transactionID;            
            if(!coordinatorMap.ContainsKey(key))
            {
                coordinatorMap.Add(key, context.coordinatorKey);
            }
            FunctionInput functionCallInput = call.funcInput;                        
            MethodInfo mi = call.type.GetMethod(call.func);
            Task<FunctionResult> t = (Task<FunctionResult>)mi.Invoke(this, new object[] { functionCallInput });
            await t;
            return t.Result;
        }

        private void Cleanup(long tid)
        {
            coordinatorMap.Remove(tid);
        }

        public async Task Abort(long tid)
        {
            //Console.WriteLine($"\n\n Grain {this.myPrimaryKey}: receives Abort message for transaction {tid}. \n\n");
            if (state == null)
                return;

            var tasks = new List<Task>();
            tasks.Add(this.state.Abort(tid));

            //Presume Abort
            //if (log != null)
                //tasks.Add(log.HandleOnAbortIn2PC(state, tid, coordinatorMap[tid]));

            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        public async Task Commit(long tid)
        {
            //Console.WriteLine($"\n\n Grain {this.myPrimaryKey}: receives Commit message for transaction {tid}. \n\n");
            if (state == null)
                return;


            var tasks = new List<Task>();
            tasks.Add(this.state.Commit(tid));
            if (log != null)
                tasks.Add(log.HandleOnCommitIn2PC(state, tid, coordinatorMap[tid]));

            Cleanup(tid);
            await Task.WhenAll(tasks);
        }

        /**
         * Stateless grain always vote "yes" for 2PC.
         */
        public async Task<bool> Prepare(long tid)
        {
            //Console.WriteLine($"\n\n Grain {this.myPrimaryKey}: receives Prepare message for transaction {tid}. \n\n");

            if (state == null)
                return true;

            var prepareResult = await this.state.Prepare(tid);
            if(prepareResult && log != null)
            if (log != null)            
                await log.HandleOnPrepareIn2PC(state, tid, coordinatorMap[tid]);
            return prepareResult;
        }
    }
}
