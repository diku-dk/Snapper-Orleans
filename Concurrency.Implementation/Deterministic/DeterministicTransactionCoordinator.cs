using Concurrency.Interface;
using Concurrency.Utilities;
using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Logging;

namespace Concurrency.Implementation.Deterministic
{
    public class DeterministicTransactionCoordinator : Grain, IDeterministicTransactionCoordinator
    {

        //Batch
        private IDisposable disposable;
        private TimeSpan waitingTime = TimeSpan.FromSeconds(1);
        private TimeSpan batchInterval = TimeSpan.FromMilliseconds(1000);

        private Dictionary<int, TransactionContext> transactionContextMap;
        private Dictionary<int, Dictionary<Guid, BatchSchedule>> batchSchedulePerGrain;
        private Dictionary<int, List<int>> batchTransactionList;
        private Dictionary<int, Dictionary<Guid, String>> batchGrainClassName;
        private ILoggingProtocol<String> log;

        protected Guid myPrimaryKey;
        //For each actor, coordinator stores the ID of the last uncommitted batch.
        //private Dictionary<IDTransactionGrain, int> actorLastBatch;

        //Maintains the number of uncompleted grains of each batch
        private Dictionary<int, int> expectedAcksPerBatch;

        //Maintains the status of batch processing
        private Dictionary<int, TaskCompletionSource<Boolean>> batchStatusMap;

        private int curBatchID { get; set; }
        private int curTransactionID { get; set; }

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
            log = new Simple2PCLoggingProtocol<String>(this.GetType().ToString(),myPrimaryKey);
            disposable = RegisterTimer(EmitBatch, null, waitingTime, batchInterval);
            return base.OnActivateAsync();
        }


        /**On receiving a new transaction request:
         * 1. Assign a batch id and transaction id to this request.
         * 
         * 2. Append the new transaction to transaction table.
         * 3. Add the new transaction to the schedule of the current batch.
         */
        public Task<TransactionContext> NewTransaction(Dictionary<Guid, Tuple<String,int>> grainAccessInformation)
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

        /** On receiving a new non-determinictic transaction request:
          * 1. Assign a transaction id to this request.
          * 2. Append the new transaction to transaction table.
          */
        public Task<TransactionContext> NewTransaction()
        {
            int tid = this.curTransactionID++;
            TransactionContext context = new TransactionContext(tid,myPrimaryKey);
            transactionContextMap.Add(tid, context);
            //Console.WriteLine($"Coordinator: received Transaction {tid}");
            return Task.FromResult(context);
        }
        
        async Task EmitBatch(object var)
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
        
        public async Task AckBatchCompletion(int bid, Guid executor_id)
        {
            if (expectedAcksPerBatch.ContainsKey(bid) && batchSchedulePerGrain[bid].ContainsKey(executor_id))
            {
                expectedAcksPerBatch[bid]--;
                if (expectedAcksPerBatch[bid] == 0)
                {
                    //Removed stale entry from actorLastBatch
                    //foreach(IDTransactionGrain grain in batchSchedulePerGrain[bid].Keys)
                    //{
                    //    if (actorLastBatch[grain] <= bid)
                    //        actorLastBatch.Remove(grain);
                    //}

                    //Remove the transaction info from coordinator state
                    if(log != null)
                        await log.HandleOnCommitInDeterministicProtocol(bid);
                    foreach (int tid in batchTransactionList[bid])
                    {
                        transactionContextMap.Remove(tid);
                    }
                    int n = batchTransactionList[bid].Count;
                    batchTransactionList.Remove(bid);
                    expectedAcksPerBatch.Remove(bid);
                    batchSchedulePerGrain.Remove(bid);
                    batchGrainClassName.Remove(bid);

                    batchStatusMap[bid].SetResult(true);
                    Console.WriteLine($"Coordinator: batch {bid} has been committed with {n} transactions. ");
                }
            }
            else
            {
                Console.WriteLine($"Coordinator: ack information from {executor_id} for batch {bid} is not correct. ");
            }
        }


        public async Task<bool> checkBatchCompletion(TransactionContext context)
        {
            if (batchStatusMap.ContainsKey(context.batchID) == false)
            {
                batchStatusMap.Add(context.batchID, new TaskCompletionSource<bool>());
            }

            if (batchStatusMap[context.batchID].Task.IsCompleted == false)
                await batchStatusMap[context.batchID].Task;
            batchStatusMap.Remove(context.batchID);
            return true;
        }

        public Task resetTimer(int period)
        {
            disposable.Dispose();
            batchInterval = TimeSpan.FromMilliseconds(period);
            disposable = RegisterTimer(EmitBatch, null, waitingTime, batchInterval);
            Console.WriteLine($"Coordinator: batch interval is set as: {period} ms.");
            return Task.CompletedTask;
        }


        Task IDeterministicTransactionCoordinator.StartAsync()
        {
            throw new NotImplementedException();
        }

        Task IDeterministicTransactionCoordinator.StopAsync()
        {
            throw new NotImplementedException();
        }
    }
}
