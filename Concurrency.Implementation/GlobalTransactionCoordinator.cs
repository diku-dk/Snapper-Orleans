using Orleans;
using Concurrency.Interface;
using System;
using System.Collections.Generic;
using System.Text;
using Concurrency.Utilities;
using System.Threading.Tasks;
using Utilities;
using Concurrency.Interface.Logging;
using System.Timers;

namespace Concurrency.Implementation
{
    public class GlobalTransactionCoordinator : Grain, IGlobalTransactionCoordinator

    {
        private int curBatchID { get; set; }
        private int curTransactionID { get; set; }
        protected Guid myPrimaryKey;
        private IGlobalTransactionCoordinator neighbour;

        //Timer
        private IDisposable disposable;
        private TimeSpan waitingTime = TimeSpan.FromSeconds(2);
        private TimeSpan batchInterval = TimeSpan.FromMilliseconds(1000);

        //Batch Schedule
        private Dictionary<int, TransactionContext> transactionContextMap;
        private Dictionary<int, Dictionary<Guid, BatchSchedule>> batchSchedulePerGrain;
        private Dictionary<int, List<int>> batchTransactionList;
        private Dictionary<int, Dictionary<Guid, String>> batchGrainClassName;



        //Maintains the status of batch processing
        private Dictionary<int, TaskCompletionSource<Boolean>> batchStatusMap;

        //Maintains the number of uncompleted grains of each batch
        private Dictionary<int, int> expectedAcksPerBatch;

        //Information controlling the emitting of non-deterministic transactions
        private int curEmitSeq { get; set; }
        Dictionary<int, TaskCompletionSource<Boolean>> emitPromiseMap;
        Dictionary<int, int> nonDeterministicEmitSize;
        //List buffering the incoming deterministic transaction requests
        Dictionary<int, List<TransactionContext>> deterministicTransactionRequests;
        int txSeqInBatch;

        //Promise controlling the emitting of token
        TaskCompletionSource<Boolean> tokenPromise;

        //Emitting status
        private Boolean isEmitTimerOn = false;
        private Boolean hasToken = false;
        private Boolean hasEmitted = false;

        private ILoggingProtocol<String> log;

        public override Task OnActivateAsync()
        {
            curBatchID = 0;
            curTransactionID = 0;

            transactionContextMap = new Dictionary<int, TransactionContext>();
            batchSchedulePerGrain = new Dictionary<int, Dictionary<Guid, BatchSchedule>>();
            batchGrainClassName = new Dictionary<int, Dictionary<Guid, String>>();
            
            //actorLastBatch = new Dictionary<IDTransactionGrain, int>();
            batchTransactionList = new Dictionary<int, List<int>>();
            expectedAcksPerBatch = new Dictionary<int, int>();
            batchStatusMap = new Dictionary<int, TaskCompletionSource<Boolean>>();
            myPrimaryKey = this.GetPrimaryKey();

            //Enable the following line for log
            //log = new Simple2PCLoggingProtocol<String>(this.GetType().ToString(), myPrimaryKey);
            disposable = RegisterTimer(EmitTransaction, null, waitingTime, batchInterval);
            tokenPromise = new TaskCompletionSource<bool>();

            curEmitSeq = 0;
            emitPromiseMap = new Dictionary<int, TaskCompletionSource<bool>>(); 
            nonDeterministicEmitSize = new Dictionary<int, int>();
            txSeqInBatch = 0;
            deterministicTransactionRequests = new Dictionary<int, List<TransactionContext>>();

            return base.OnActivateAsync();
        }

        /**
         *Client calls this function to submit deterministic transaction
         */
        public async Task<TransactionContext> NewTransaction(Dictionary<Guid, Tuple<string, int>> grainAccessInformation)
        {

            int index = txSeqInBatch++;
            int myEmitSeq = this.curEmitSeq;
            if (deterministicTransactionRequests.ContainsKey(myEmitSeq) == false)
                deterministicTransactionRequests.Add(myEmitSeq, new List<TransactionContext>());
            deterministicTransactionRequests[myEmitSeq].Add(new TransactionContext(grainAccessInformation));

            TaskCompletionSource<bool> emitting;
            if (!emitPromiseMap.ContainsKey(myEmitSeq))
            {
                emitPromiseMap.Add(myEmitSeq, new TaskCompletionSource<bool>());
            }
            emitting = emitPromiseMap[myEmitSeq];
            if (emitting.Task.IsCompleted != true)
            {
                await emitting.Task;
            }
            return deterministicTransactionRequests[myEmitSeq][index];
        }


        /**
         *Client calls this function to submit non-deterministic transaction
         */
        public async Task<TransactionContext> NewTransaction()
        {
            TaskCompletionSource<bool> emitting;
            int myEmitSeq = this.curEmitSeq;
            if (!emitPromiseMap.ContainsKey(myEmitSeq))
            {
                emitPromiseMap.Add(myEmitSeq, new TaskCompletionSource<bool>());
                this.nonDeterministicEmitSize.Add(myEmitSeq, 0);
            }       
            emitting = emitPromiseMap[myEmitSeq];
            nonDeterministicEmitSize[myEmitSeq] = nonDeterministicEmitSize[myEmitSeq] + 1;

            if (emitting.Task.IsCompleted != true)
            {
                await emitting.Task;
            }
            int tid = this.curTransactionID++;
            TransactionContext context = new TransactionContext(tid, myPrimaryKey);

            //Check if the emitting of the current non-deterministic batch is completed, if so, 
            //set the token promise and increment the curNondeterministicBatchID.
            nonDeterministicEmitSize[myEmitSeq] = nonDeterministicEmitSize[myEmitSeq] - 1;
            if(nonDeterministicEmitSize[myEmitSeq] == 0)
            {
                tokenPromise.SetResult(true); //Note: the passing of token could interleave here
                nonDeterministicEmitSize.Remove(myEmitSeq);
                emitPromiseMap.Remove(myEmitSeq);
            }

            //Console.WriteLine($"Coordinator: received Transaction {tid}");
            return context;
        }

        /**
         *Coordinator calls this function to pass token
         */
        public async Task PassToken(BatchToken token)
        {
            this.hasToken = true;
            await EmitTransaction(token);

            if (this.hasEmitted)
            {
                //Update the token and pass it to the neighbour coordinator
                token.lastBatchID = this.curBatchID - 1;
                token.lastTransactionID = this.curTransactionID - 1;
                this.hasEmitted = false;
            }
            neighbour.PassToken(token);           
        }

        /**
         *This functions is called when a new token is received or the timer is set
         */
        async Task EmitTransaction(Object obj)
        {
            BatchToken token = (BatchToken)obj;
            if (hasToken == false && token == null)
            {
                this.isEmitTimerOn = true;
                return;
            }
            else if(this.isEmitTimerOn == false)
            {
                return;
            }
  
            await EmitBatch(token);


            if (emitPromiseMap.ContainsKey(curEmitSeq))
            {
                emitPromiseMap[curEmitSeq].SetResult(true);
            }
            else
            {
                if (tokenPromise.Task.IsCompleted == false)
                    tokenPromise.SetResult(true);
            }

            await tokenPromise.Task;

            this.isEmitTimerOn = false;
            this.hasToken = false;
            this.hasEmitted = true;
            tokenPromise = new TaskCompletionSource<bool>();
            return;
        }

        /**
         *This functions is called to emit batch of deterministic transactions
         */
        async Task EmitBatch(BatchToken token)
        {
            int myEmitSequence = this.curEmitSeq++;
            txSeqInBatch = 0;
            curBatchID = token.lastBatchID + 1;
            curTransactionID = token.lastTransactionID + 1;

            List<TransactionContext> transactionList;
            Boolean shouldEmit = deterministicTransactionRequests.TryGetValue(myEmitSequence, out transactionList);

            //Return if there is no deterministic transactions waiting for emit
            if (shouldEmit == false)
                return;

            foreach(TransactionContext context in transactionList)
            {
                context.batchID = curBatchID;
                context.transactionID = curTransactionID++;

                transactionContextMap.Add(context.transactionID, context);

                //update batch schedule
                if (batchSchedulePerGrain.ContainsKey(context.batchID) == false)
                {
                    batchSchedulePerGrain.Add(context.batchID, new Dictionary<Guid, BatchSchedule>());
                    batchTransactionList.Add(context.batchID, new List<int>());
                    batchGrainClassName.Add(context.batchID, new Dictionary<Guid, String>());
                }
    
                batchTransactionList[context.batchID].Add(context.transactionID);

                //update the schedule for each grain accessed by this transaction
                Dictionary<Guid, BatchSchedule> grainSchedule = batchSchedulePerGrain[context.batchID];
                foreach (var item in context.grainAccessInformation)
                {
                    batchGrainClassName[context.batchID][item.Key] = item.Value.Item1;
                    if (grainSchedule.ContainsKey(item.Key) == false)
                        grainSchedule.Add(item.Key, new BatchSchedule(context.batchID));
                    grainSchedule[item.Key].AddNewTransaction(context.transactionID, item.Value.Item2);
                }
                context.grainAccessInformation.Clear();
            }

            Dictionary<Guid, BatchSchedule> curScheduleMap = batchSchedulePerGrain[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, curScheduleMap.Count);

            if (batchStatusMap.ContainsKey(curBatchID) == false)
                batchStatusMap.Add(curBatchID, new TaskCompletionSource<Boolean>());

            var v = typeof(IDeterministicTransactionCoordinator);
            if (log != null)
            {
                HashSet<Guid> participants = new HashSet<Guid>();
                participants.UnionWith(curScheduleMap.Keys);
                await log.HandleOnPrepareInDeterministicProtocol(curBatchID, participants);
            }
            foreach (KeyValuePair<Guid, BatchSchedule> item in curScheduleMap)
            {
                var dest = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, batchGrainClassName[curBatchID][item.Key]);
                BatchSchedule schedule = item.Value;
                Task emit = dest.ReceiveBatchSchedule(schedule);
            }
            batchGrainClassName.Remove(curBatchID);
            //Console.WriteLine($"Coordinator: sent schedule for batch {curBatchID} to {curScheduleMap.Count} grains.");
        }

        public Task AckTransactionCompletion(int bid, Guid executor_id)
        {
            throw new NotImplementedException();
        }

        public async Task SpawnCoordinator(uint myId, uint neighbourId)
        {
            neighbour = this.GrainFactory.GetGrain<IGlobalTransactionCoordinator>(Helper.convertUInt32ToGuid(neighbourId));
            //The "first" coordinator starts the token passing
            if (myId == 0)
            {
                System.Threading.Thread.Sleep(2000);
                BatchToken token = new BatchToken(-1, -1);
                EmitTransaction(token);
            }
        }
    }
}
