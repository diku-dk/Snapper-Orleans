using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Concurrency.Interface;
using Concurrency.Utilities;
using Orleans;

namespace Concurrency.Implementation
{
    public abstract class TransactionExecutionGrain : Grain, ITransactionExecutionGrain
    {
        public IDeterministicTransactionCoordinator tc;
        private int lastBatchId;
        private Dictionary<int, BatchSchedule> batchScheduleMap;
        private Queue<BatchSchedule> batchQueue;
        private Dictionary<int, List<TaskCompletionSource<Boolean>>> promiseMap;
        public override Task OnActivateAsync()
        {
            tc = this.GrainFactory.GetGrain<IDeterministicTransactionCoordinator>(0);
            batchQueue = new Queue<BatchSchedule>();
            batchScheduleMap = new Dictionary<int, BatchSchedule>();

            //Initialize the "batch" with id "-1" as completed;
            lastBatchId = -1;
            batchScheduleMap.Add(lastBatchId, new BatchSchedule(lastBatchId, true));

            promiseMap = new Dictionary<int, List<TaskCompletionSource<Boolean>>>();
            return base.OnActivateAsync();
        }


        /**
         * Submit a determinictic transaction to the coordinator. 
         * On receiving the returned transaction context, start the execution of a transaction.
         * 
         */

        public async Task<List<object>> StartTransaction(Dictionary<ITransactionExecutionGrain, int> grainToAccessTimes, String startFunction, List<object> inputs)
        {
            TransactionContext context = await tc.NewTransaction(grainToAccessTimes);
            inputs.Insert(0, context);
            FunctionCall c1 = new FunctionCall(context, this.GetType(), startFunction, inputs);
            Task<List<object>> t1 = this.Execute(c1);
            Task t2 = tc.checkBatchCompletion(context);
            await Task.WhenAll(t1, t2);
            return t1.Result;

        }

        public async Task<List<object>> StartTransaction(String startFunction, List<object> inputs)
        {
            TransactionContext context = await tc.NewTransaction();
            inputs.Insert(0, context);
            FunctionCall c1 = new FunctionCall(context, this.GetType(), startFunction, inputs);
            Task<List<object>> t1 = this.InvokeFunction(c1);
            await t1;

            // Prepare Phase
            List<Task<Boolean>> prepareResult = new List<Task<Boolean>>();
            foreach (object obj in t1.Result)
            {
                ITransactionExecutionGrain grain = (ITransactionExecutionGrain) obj;
                prepareResult.Add(grain.Prepare(context.transactionID));
            }
            await Task.WhenAll(prepareResult);
            bool canCommit = true;
            foreach(Task<Boolean> vote in prepareResult){
                if(vote.Result == false)
                {
                    canCommit = false;
                    break;
                }
            }

            // Commit / Abort Phase
            List<Task> commitResult = new List<Task>();
            if (canCommit)
            {
                foreach (object obj in t1.Result)
                {
                    ITransactionExecutionGrain grain = (ITransactionExecutionGrain)obj;
                    commitResult.Add(grain.Commit(context.transactionID));
                }
            }
            else
            {
                foreach (object obj in t1.Result)
                {
                    ITransactionExecutionGrain grain = (ITransactionExecutionGrain)obj;
                    commitResult.Add(grain.Abort(context.transactionID));
                }
            }
            await Task.WhenAll(commitResult);
            return t1.Result;
        }

        /**
         * On receive the schedule for a specific batch
         * 1. Store this schedule.
         * 2. Check if there is function call that should be executed now, and execute it if yes.
         *
         */

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task ReceiveBatchSchedule(BatchSchedule schedule)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            //Console.WriteLine($"\n\n{this.GetType()}: Received schedule for batch {schedule.batchID}.\n\n");
            if (batchScheduleMap.ContainsKey(schedule.batchID))
            {
                //Console.WriteLine($"\n\n The key {schedule.batchID} is existed.\n\n");
            }
            else
            {
                BatchSchedule curSchedule = new BatchSchedule(schedule);
                batchScheduleMap.Add(schedule.batchID, curSchedule);
                batchQueue.Enqueue(curSchedule);
            }

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
                else
                {
                    //Console.WriteLine($"\n\n{this.GetType()}: Promise for Tx {tid} doesn't exist, size of promiseMap: {promiseMap.Count}.\n\n");
                }
            }

            return;
        }

        /**
         *Allow reentrance to enforce ordered execution
         */
        public async Task<List<object>> Execute(FunctionCall call)
        {
            int tid = call.context.transactionID;
            int bid = call.context.batchID;
            int nextTid;
            //Console.WriteLine($"\n\n{this.GetType()}: Execute batch {bid} transaction {tid}. \n\n");

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

            List<object> ret = new List<object>();

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

            //set promise for the next call if existed.

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

                //TODO: remove state about the completed batch state?
                Task ack = tc.AckBatchCompletion(bid, this.AsReference<ITransactionExecutionGrain>());

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

            return ret;
        }

        public async Task<List<object>> InvokeFunction(FunctionCall call)
        {
            List<object> inputs = call.inputs;
            MethodInfo mi = call.type.GetMethod(call.func);
            Task<List<object>> t = (Task<List<object>>)mi.Invoke(this, new object[] { inputs });
            await t;
            return t.Result;
        }

        public Task Abort(long tid)
        {
            throw new NotImplementedException();
        }

        public Task Commit(long tid)
        {
            throw new NotImplementedException();
        }

        public Task<bool> Prepare(long tid)
        {
            throw new NotImplementedException();
        }

        public Task ActivateGrain()
        {
            return Task.CompletedTask;
        }
    }



}
