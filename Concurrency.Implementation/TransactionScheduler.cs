using Utilities;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Implementation
{


    class TransactionScheduler
    {        
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        private ScheduleInfo scheduleInfo;
        //private int lastScheduledBatchId; //Includes deterministic and not-deterministic batches
        //private TaskCompletionSource<Boolean> nonDetCompletion;
        private Dictionary<int, Dictionary<int, List<TaskCompletionSource<Boolean>>>> inBatchTransactionCompletionMap;


        public TransactionScheduler(Dictionary<int, DeterministicBatchSchedule> batchScheduleMap)
        {
            this.batchScheduleMap = batchScheduleMap;            
            inBatchTransactionCompletionMap = new Dictionary<int, Dictionary<int, List<TaskCompletionSource<bool>>>>();
            scheduleInfo = new ScheduleInfo();
        }

        public async void RegisterDeterministicBatchSchedule(int batchID) 
        {
            var schedule = batchScheduleMap[batchID];
            //Create the promise of the previous batch if not present

            scheduleInfo.insertDetBatch(schedule);
            //Create the in batch promise map if not present
            if (this.inBatchTransactionCompletionMap.ContainsKey(schedule.batchID) == false)
                this.inBatchTransactionCompletionMap.Add(schedule.batchID, new Dictionary<int, List<TaskCompletionSource<bool>>>());

            //Check if this batch can be executed: 
            //(1) check the promise status of its previous batch
            //(2) check the promise for nonDeterministic batch
            await scheduleInfo.getDependingPromise(schedule.batchID).Task;

            //Check if there is a buffered function call for this batch, if present, execute it
            int tid = schedule.curExecTransaction();
            //Console.WriteLine($"\n{this.GetType()}: next transaction to be executed is {tid}.\n");
            if (inBatchTransactionCompletionMap[schedule.batchID].ContainsKey(tid) && inBatchTransactionCompletionMap[schedule.batchID][tid].Count != 0)
            {
                if (inBatchTransactionCompletionMap[schedule.batchID][tid][0].Task.IsCompleted == false)
                       inBatchTransactionCompletionMap[schedule.batchID][tid][0].SetResult(true);
            }
        }

        //for deterministic transactions
        public async Task<int> waitForTurn(int bid, int tid)
        {
            //Check if the list of promises for this transaction exsists, if not, add it.
            if (inBatchTransactionCompletionMap.ContainsKey(bid) == false)
            {
                inBatchTransactionCompletionMap.Add(bid, new Dictionary<int, List<TaskCompletionSource<bool>>>());
            }
            if (inBatchTransactionCompletionMap[bid].ContainsKey(tid) == false)
            {
                inBatchTransactionCompletionMap[bid].Add(tid, new List<TaskCompletionSource<bool>>());
                //This promise is for the "-1" function call in this transaction
                inBatchTransactionCompletionMap[bid][tid].Add(new TaskCompletionSource<bool>());
            }

            //This function call waits for the last promise in the list
            int count = inBatchTransactionCompletionMap[bid][tid].Count;
            var lastPromise = inBatchTransactionCompletionMap[bid][tid][count - 1];
            //Console.WriteLine($"Transaction {bid}:{tid} is waiting for turn {count - 1}");

            //Promise created for the current execution, which is waited by the next function call
            var myPromise = new TaskCompletionSource<Boolean>();
            inBatchTransactionCompletionMap[bid][tid].Add(myPromise);

            //Check if this call can be executed;
            if (batchScheduleMap.ContainsKey(bid))
            {
                DeterministicBatchSchedule schedule = batchScheduleMap[bid];
                //TODO: XXX: Assumption right now is that all non-deterministic transactions will execute as one big batch
                if(scheduleInfo.getDependingPromise(schedule.batchID).Task.IsCompleted == false)
                {
                    //If it is not the trun for this batch, then await or its turn
                    //Console.WriteLine($"Transaction {bid}:{tid}, waiting for batch {schedule.lastBatchID}.");
                    await scheduleInfo.getDependingPromise(schedule.batchID).Task;
                    //Console.WriteLine($"Transaction {bid}:{tid} passed point A");
                }
                //Check if this transaction cen be executed
                int nextTid = batchScheduleMap[bid].curExecTransaction();
                if (tid == nextTid)
                {
                    //Console.WriteLine($"\n\n{this.GetType()}: Set Promise for Tx: {tid} in batch {bid} within Execute(). \n\n");
                    //Set the promise for the first function call of this Transaction if it is not
                    if (inBatchTransactionCompletionMap[bid][tid][0].Task.IsCompleted == false) { 
                        inBatchTransactionCompletionMap[bid][tid][0].SetResult(true);
                        //Console.WriteLine($"Transaction {bid}:{tid} set turn {0} as true.");
                    }
                }
            }

            if (lastPromise.Task.IsCompleted == false)
                await lastPromise.Task;
            //Console.WriteLine($"Transaction {bid}:{tid} passed point B");
            //count identifies the location of the promise for this turn
            return count;
        }

        //For nonDeterninistic Transactions
        public async Task waitForTurn(int tid)
        {
            await scheduleInfo.InsertNonDetTransaction(tid).promise.Task;
        }

        //For deterministic transaction
        public bool ackComplete(int bid, int tid, int turnIndex)
        {
            DeterministicBatchSchedule schedule = batchScheduleMap[bid];
            schedule.AccessIncrement(tid);
            bool switchingBatches = false;

            //Find the next transaction to be executed in this batch;
            int nextTid = schedule.curExecTransaction();
            if(tid == nextTid)
            {
                //Within the same batch same transaction

                inBatchTransactionCompletionMap[bid][tid][turnIndex].SetResult(true);
                //Console.WriteLine($"Transaction {bid}:{tid} sets turn {turnIndex} as true.");
            }
            else if (nextTid != -1)
            {
                //Within the same batch but switching to another transaction
                if (!inBatchTransactionCompletionMap[bid].ContainsKey(nextTid))
                {
                    //Console.WriteLine($"\n\n{this.GetType()}: Set promise result for Tx {nextTid} \n\n");
                    inBatchTransactionCompletionMap[bid].Add(nextTid, new List<TaskCompletionSource<bool>>());
                    inBatchTransactionCompletionMap[bid][nextTid].Add(new TaskCompletionSource<bool>(false));
                }
                //Console.WriteLine($"Transaction {bid}:{nextTid} sets turn {0} as true.");
                inBatchTransactionCompletionMap[bid][nextTid][0].SetResult(true);
            }
            else
            {
                //Finished the batch, need to switch to another batch or non-deterministic transaction                
                switchingBatches = true;
                //Log the state now                                
                //The schedule for this batch {$bid} has been completely executed. Check if any promise for next batch can be set.
                this.scheduleInfo.completeDeterministicBatch(bid);
                //TODO: XXX Remember to garbage collect promises
            }
            return switchingBatches;
        }

        //Called to notify the completion of non-deterministic transaction
        public void ackComplete(int tid)
        {
            scheduleInfo.completeTransaction(tid);
        }

        public void ackBatchCommit(int bid)
        {
            scheduleInfo.removePreviousNodes(bid);
            var schedule = batchScheduleMap[batchScheduleMap[bid].lastBatchID];
            bid = schedule.batchID;
            while(bid != -1 && schedule != null)
            {                
                inBatchTransactionCompletionMap.Remove(bid);
                batchScheduleMap.Remove(bid);
                bid = schedule.lastBatchID;                             
                schedule = batchScheduleMap[bid];
            }
        }

        public HashSet<int> getBeforeSet(int tid, out int maxBeforeBid)
        {
            return scheduleInfo.getBeforeSet(tid, out maxBeforeBid);
        }

        public HashSet<int> getAfterSet(int tid, out int minAfterBid)
        {
            return scheduleInfo.getAfterSet(tid, out minAfterBid);
        }
    }


}
