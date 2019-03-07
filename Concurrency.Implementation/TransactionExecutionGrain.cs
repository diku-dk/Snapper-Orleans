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
        //public INondeterministicTransactionCoordinator ndtc;
        //public IDeterministicTransactionCoordinator dtc;
        private int lastBatchId;
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        private Dictionary<long, Guid> coordinatorMap;
        //private Queue<DeterministicBatchSchedule> batchScheduleQueue;
        private NonDeterministicSchedule nonDetSchedule;
        private Dictionary<int, TaskCompletionSource<Boolean>> batchCompletionMap;
        private TaskCompletionSource<Boolean> nonDetCompletion;
        private Dictionary<int, Dictionary<int, List<TaskCompletionSource<Boolean>>>> inBatchTransactionCompletionMap;
        protected Guid myPrimaryKey;
        protected ITransactionalState<TState> state;
        protected ILoggingProtocol<TState> log = null;
        protected String myUserClassName;
        protected int numCoordinators = 1;
        protected Random rnd;
        private IGlobalTransactionCoordinator myCoordinator;

        public TransactionExecutionGrain(ITransactionalState<TState> state){
            this.state = state;
        }

        public TransactionExecutionGrain()
        {

        }
        public override Task OnActivateAsync()
        {
            rnd = new Random();
            myCoordinator = this.GrainFactory.GetGrain<IGlobalTransactionCoordinator>(Helper.convertUInt32ToGuid((UInt32)rnd.Next(0, numCoordinators)));
            //ndtc = this.GrainFactory.GetGrain<INondeterministicTransactionCoordinator>(Helper.convertUInt32ToGuid(0));
            //dtc = this.GrainFactory.GetGrain<IDeterministicTransactionCoordinator>(Helper.convertUInt32ToGuid(0));
            //batchScheduleQueue = new Queue<DeterministicBatchSchedule>();
            batchScheduleMap = new Dictionary<int, DeterministicBatchSchedule>();

            //Initialize the "batch" with id "-1" as completed;
            lastBatchId = -1;
            batchScheduleMap.Add(lastBatchId, new DeterministicBatchSchedule(lastBatchId, true));

            inBatchTransactionCompletionMap = new Dictionary<int, Dictionary<int, List<TaskCompletionSource<bool>>>>();
            myPrimaryKey = this.GetPrimaryKey();
            //Enable the following line for logging

            //log = new Simple2PCLoggingProtocol<TState>(this.GetType().ToString(), myPrimaryKey);
            batchCompletionMap = new Dictionary<int, TaskCompletionSource<Boolean>>();
            batchCompletionMap.Add(-1, new TaskCompletionSource<Boolean>(true));
            nonDetCompletion = new TaskCompletionSource<bool>(false);
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
            TransactionContext context = await myCoordinator.NewTransaction(grainAccessInformation);
            inputs.context = context;
            FunctionCall c1 = new FunctionCall(this.GetType(), startFunction, inputs);
            
            Task<FunctionResult> t1 = this.Execute(c1);
            Task t2 = myCoordinator.checkBatchCompletion(context);
            await Task.WhenAll(t1, t2);
            //Console.WriteLine($"Transaction {context.transactionID}: completed executing.\n");
            return t1.Result;
        }

        public async Task<FunctionResult> StartTransaction(String startFunction, FunctionInput functionCallInput)
        {
            TransactionContext context = await myCoordinator.NewTransaction();
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
                Task logTask = Task.CompletedTask;
                if (log != null)
                    logTask = log.HandleBeforePrepareIn2PC(context.transactionID, context.coordinatorKey, participants);

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
         
        public Task ReceiveBatchSchedule(DeterministicBatchSchedule schedule)
        {
            //Console.WriteLine($"\n\n{this.GetType()}: Received schedule for batch {schedule.batchID}.\n\n");        
            
            batchScheduleMap.Add(schedule.batchID, schedule);
            //batchScheduleQueue.Enqueue(schedule);

            //Create the promise of the previous batch if not present
            if(!batchCompletionMap.ContainsKey(schedule.lastBatchID))
            {
                batchCompletionMap.Add(schedule.lastBatchID, new TaskCompletionSource<Boolean>(false));
            }
            //Create my own promise if not present
            if (!batchCompletionMap.ContainsKey(schedule.batchID))
            {
                batchCompletionMap.Add(schedule.batchID, new TaskCompletionSource<Boolean>(false));
            }

            //Create the in batch promise map if not present
            if (this.inBatchTransactionCompletionMap.ContainsKey(schedule.batchID) == false)
                this.inBatchTransactionCompletionMap.Add(schedule.batchID, new Dictionary<int, List<TaskCompletionSource<bool>>>());

            //Check if this batch can be executed: 
            //(1) check the promise status of its previous batch
            //(2) check the promise for nonDeterministic batch
            if(this.nonDetCompletion.Task.IsCompleted && batchCompletionMap[schedule.lastBatchID].Task.IsCompleted)
            {
                //Check if there is a buffered function call for this batch, if present, execute it
                int tid = schedule.curExecTransaction();
                if ( inBatchTransactionCompletionMap[schedule.batchID].ContainsKey(tid) && inBatchTransactionCompletionMap[schedule.batchID][tid].Count != 0)
                {
                    inBatchTransactionCompletionMap[schedule.batchID][tid][0].SetResult(true);
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

            
            int tid = call.funcInput.context.inBatchTransactionID;
            int bid = call.funcInput.context.batchID;
            // await scheduler.waitForTurn(bid, tid);
            
            //Execute the function call;
            var ret = await InvokeFunction(call);


            //Console.WriteLine($"\n\n{this.GetType()}:  Tx {tid} is executed... trying next Tx ... \n\n");



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

        private void Cleanup(long bid)
        {
            coordinatorMap.Remove(bid);
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
