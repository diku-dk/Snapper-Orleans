using Concurrency.Interface;
using Concurrency.Utilities;
using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Implementation.Deterministic
{
    [Serializable]
    public class DeterministicTransactionCoordinator : Grain, IDeterministicTransactionCoordinator
    {

        //Batch
        private IDisposable disposable;
        private TimeSpan waitingTime = TimeSpan.FromSeconds(1);
        private TimeSpan batchInterval = TimeSpan.FromMilliseconds(1000);

        private Dictionary<int, TransactionContext> transactionContextMap;
        private Dictionary<int, Dictionary<ITransactionExecutionGrain, BatchSchedule>> batchSchedulePerGrain;
        private Dictionary<int, List<int>> batchTransactionList;

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
            batchSchedulePerGrain = new Dictionary<int, Dictionary<ITransactionExecutionGrain, BatchSchedule>>();
            //actorLastBatch = new Dictionary<IDTransactionGrain, int>();
            batchTransactionList = new Dictionary<int, List<int>>();
            expectedAcksPerBatch = new Dictionary<int, int>();
            batchStatusMap = new Dictionary<int, TaskCompletionSource<Boolean>>();

            disposable = RegisterTimer(EmitBatch, null, waitingTime, batchInterval);
            return base.OnActivateAsync();
        }


        /**On receiving a new transaction request:
         * 1. Assign a batch id and transaction id to this request.
         * 
         * 2. Append the new transaction to transaction table.
         * 3. Add the new transaction to the schedule of the current batch.
         */
        public Task<TransactionContext> NewTransaction(Dictionary<ITransactionExecutionGrain, int> grainToAccessTimes)
        {
            int bid = this.curBatchID;
            int tid = this.curTransactionID++;
            TransactionContext context = new TransactionContext(bid, tid);

            transactionContextMap.Add(tid, context);

            //update batch schedule
            if (batchSchedulePerGrain.ContainsKey(bid) == false)
            {
                batchSchedulePerGrain.Add(bid, new Dictionary<ITransactionExecutionGrain, BatchSchedule>());
                batchTransactionList.Add(bid, new List<int>());
            }

            batchTransactionList[bid].Add(tid);

            Dictionary<ITransactionExecutionGrain, BatchSchedule> curScheduleMap = batchSchedulePerGrain[bid];
            foreach (KeyValuePair<ITransactionExecutionGrain, int> item in grainToAccessTimes)
            {
                //if (actorLastBatch.ContainsKey(item.Key))
                //    lastBid = actorLastBatch[item.Key];
                //else
                //    lastBid = -1;
                if (curScheduleMap.ContainsKey(item.Key) == false)
                    curScheduleMap.Add(item.Key, new BatchSchedule(bid));
                curScheduleMap[item.Key].AddNewTransaction(tid, item.Value);
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
            TransactionContext context = new TransactionContext(tid);
            transactionContextMap.Add(tid, context);
            //Console.WriteLine($"Coordinator: received Transaction {tid}");
            return Task.FromResult(context);
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        async Task EmitBatch(object var)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            if (batchSchedulePerGrain.ContainsKey(curBatchID) == false)
                return;


            Dictionary<ITransactionExecutionGrain, BatchSchedule> curScheduleMap = batchSchedulePerGrain[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, curScheduleMap.Count);

            if (batchStatusMap.ContainsKey(curBatchID) == false)
                batchStatusMap.Add(curBatchID, new TaskCompletionSource<Boolean>());

            foreach (KeyValuePair<ITransactionExecutionGrain, BatchSchedule> item in curScheduleMap)
            {
                ITransactionExecutionGrain dest = item.Key;
                BatchSchedule schedule = item.Value;
                Task emit = dest.ReceiveBatchSchedule(schedule);

                //if(actorLastBatch.ContainsKey(dest))
                //    actorLastBatch[dest] = schedule.batchID;
                //else
                //    actorLastBatch.Add(dest, schedule.batchID);
            }

            //This guarantees that batch schedules with smaller IDs are received earlier by grains.
            //await Task.WhenAll(emitTasks);
            //Console.WriteLine($"Coordinator: sent schedule for batch {curBatchID} to {curScheduleMap.Count} grains.");
            curBatchID++;
            return;
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task AckBatchCompletion(int bid, ITransactionExecutionGrain executor)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            if (expectedAcksPerBatch.ContainsKey(bid) && batchSchedulePerGrain[bid].ContainsKey(executor))
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

                    foreach (int tid in batchTransactionList[bid])
                    {
                        transactionContextMap.Remove(tid);
                    }
                    int n = batchTransactionList[bid].Count;
                    batchTransactionList.Remove(bid);
                    expectedAcksPerBatch.Remove(bid);
                    batchSchedulePerGrain.Remove(bid);

                    batchStatusMap[bid].SetResult(true);
                    Console.WriteLine($"Coordinator: batch {bid} has been committed with {n} transactions. ");
                }
            }
            else
            {
                Console.WriteLine($"Coordinator: ack information from {executor} for batch {bid} is not correct. ");
            }
            return;
        }


        public async Task<bool> checkBatchCompletion(TransactionContext context)
        {
            if (batchStatusMap.ContainsKey(context.batchID) == false)
            {
                batchStatusMap.Add(context.batchID, new TaskCompletionSource<bool>());
            }

            if (batchStatusMap[context.batchID].Task.IsCompleted == false)
                await batchStatusMap[context.batchID].Task;

            return batchStatusMap[context.batchID].Task.Result;
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
