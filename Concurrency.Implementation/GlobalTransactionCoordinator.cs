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
        private int curNondeterministicBatchID { get; set; }
        Dictionary<int, TaskCompletionSource<Boolean>> nonDeterministicPromiseMap;
        Dictionary<int, int> nonDeterministicBatchSize;

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
            curNondeterministicBatchID = 0;
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
            nonDeterministicPromiseMap = new Dictionary<int, TaskCompletionSource<bool>>();
            nonDeterministicBatchSize = new Dictionary<int, int>();

            return base.OnActivateAsync();
        }

        /**
         *Client calls this function to submit deterministic transaction
         */
        public Task<TransactionContext> NewTransaction(Dictionary<Guid, Tuple<string, int>> grainAccessInformation)
        {
            int bid = this.curBatchID;
            int tid = this.curTransactionID++;
            TransactionContext context = new TransactionContext(bid, tid, myPrimaryKey);

            transactionContextMap.Add(tid, context);

            //update batch schedule
            if (batchSchedulePerGrain.ContainsKey(bid) == false)
            {
                batchSchedulePerGrain.Add(bid, new Dictionary<Guid, BatchSchedule>());
                batchTransactionList.Add(bid, new List<int>());
                batchGrainClassName.Add(bid, new Dictionary<Guid, String>());
            }

            batchTransactionList[bid].Add(tid);

            Dictionary<Guid, BatchSchedule> curScheduleMap = batchSchedulePerGrain[bid];
            foreach (var item in grainAccessInformation)
            {
                //if (actorLastBatch.ContainsKey(item.Key))
                //    lastBid = actorLastBatch[item.Key];
                //else
                //    lastBid = -1;
                batchGrainClassName[bid][item.Key] = item.Value.Item1;
                if (curScheduleMap.ContainsKey(item.Key) == false)
                    curScheduleMap.Add(item.Key, new BatchSchedule(bid));
                curScheduleMap[item.Key].AddNewTransaction(tid, item.Value.Item2);
            }

            //Console.WriteLine($"Coordinator: received Transaction {tid} for Batch {curBatchID}.");
            return Task.FromResult(context);
        }


        /**
         *Client calls this function to submit non-deterministic transaction
         */
        public async Task<TransactionContext> NewTransaction()
        {
            TaskCompletionSource<bool> emitting;
            if (!nonDeterministicPromiseMap.ContainsKey(curNondeterministicBatchID))
            {
                nonDeterministicPromiseMap.Add(curNondeterministicBatchID, new TaskCompletionSource<bool>());
                this.nonDeterministicBatchSize.Add(curNondeterministicBatchID, 0);
            }
                
            emitting = nonDeterministicPromiseMap[curNondeterministicBatchID];
            nonDeterministicBatchSize[curNondeterministicBatchID] = nonDeterministicBatchSize[curNondeterministicBatchID] + 1;

            if (emitting.Task.IsCompleted != true)
            {
                await emitting.Task;
            }
            int tid = this.curTransactionID++;
            TransactionContext context = new TransactionContext(tid, myPrimaryKey);

            //Check if the emitting of the current non-deterministic batch is completed, if so, 
            //set the token promise and increment the curNondeterministicBatchID.
            nonDeterministicBatchSize[curNondeterministicBatchID] = nonDeterministicBatchSize[curNondeterministicBatchID] - 1;
            if(nonDeterministicBatchSize[curNondeterministicBatchID] == 0)
            {
                this.tokenPromise.SetResult(true); //Note: the passing of token could interleave here
                nonDeterministicBatchSize.Remove(curNondeterministicBatchID);
                this.nonDeterministicPromiseMap.Remove(curNondeterministicBatchID);
                curNondeterministicBatchID++;
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
            if (this.nonDeterministicPromiseMap.ContainsKey(curNondeterministicBatchID))
            {
                this.nonDeterministicPromiseMap[this.curNondeterministicBatchID].SetResult(true);
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
            if (batchSchedulePerGrain.ContainsKey(curBatchID) == false)
                return;

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

                //if(actorLastBatch.ContainsKey(dest))
                //    actorLastBatch[dest] = schedule.batchID;
                //else
                //    actorLastBatch.Add(dest, schedule.batchID);
            }
            batchGrainClassName.Remove(curBatchID);
            //This guarantees that batch schedules with smaller IDs are received earlier by grains.
            //await Task.WhenAll(emitTasks);
            //Console.WriteLine($"Coordinator: sent schedule for batch {curBatchID} to {curScheduleMap.Count} grains.");
            curBatchID++;
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
