using Utilities;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Interface;
using Orleans;
using System.Diagnostics;

namespace Concurrency.Implementation
{


    class TransactionScheduler
    {        
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        private ScheduleInfo scheduleInfo;
        //private int lastScheduledBatchId; //Includes deterministic and not-deterministic batches
        //private TaskCompletionSource<Boolean> nonDetCompletion;
        private Dictionary<int, Dictionary<int, List<TaskCompletionSource<Boolean>>>> inBatchTransactionCompletionMap;
        private int maxNonDetWaitingLatencyInMs;
        IGrainFactory grainFactory;

        public TransactionScheduler(Dictionary<int, DeterministicBatchSchedule> batchScheduleMap, int latency, IGrainFactory grainFactory)
        {
            this.batchScheduleMap = batchScheduleMap;            
            inBatchTransactionCompletionMap = new Dictionary<int, Dictionary<int, List<TaskCompletionSource<bool>>>>();
            scheduleInfo = new ScheduleInfo();
            maxNonDetWaitingLatencyInMs = latency;
            this.grainFactory = grainFactory;
        }

        // this function is called only when want to commit a nond-det transaction
        public async Task waitForBatchCommit(int batchId)
        {
            
            if (scheduleInfo.nodes.ContainsKey(batchId))
            {
                var task = scheduleInfo.nodes[batchId].commitmentPromise.Task;
                if (task.IsCompleted)
                    return;
                else
                {
                    await Task.WhenAny(Task.Delay(maxNonDetWaitingLatencyInMs), task);
                }
                if (task.IsCompleted)
                    return;
                else
                {
                    Debug.Assert(batchScheduleMap.ContainsKey(batchId));
                    var coordinatorID = batchScheduleMap[batchId].globalCoordinator;
                    IGlobalTransactionCoordinatorGrain coordinator = grainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(coordinatorID);
                    await coordinator.checkBatchCompletion(batchId);
                }
                    
                return;

            }
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
            await scheduleInfo.InsertNonDetTransaction(tid).executionPromise.Task;
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

        // TODO: changed by Yijian
        // this function is only used to do grabage collection
        // bid: current highest committed batch among all coordinators
        public void ackBatchCommit(int bid)
        {
            try
            {
                int batchID = -1;
                foreach (var key in scheduleInfo.nodes.Keys)
                {
                    if (key <= bid && scheduleInfo.nodes[key].isDet) batchID = Math.Max(batchID, key);
                }
                if (batchID == -1 || scheduleInfo.nodes[batchID].commitmentPromise.Task.IsCompleted) return;
                // commitment promise is used when non-det txn is waiting for a batch commits
                scheduleInfo.nodes[batchID].commitmentPromise.SetResult(true);
                // TODO: remove all nodes before batchID, not including batchID, why (Yijian)
                scheduleInfo.removePreviousNodes(batchID);
                var lastBid = batchScheduleMap[batchID].lastBatchID;
                // if not contains the key lastBid, it means another thread before this one has removed this batch
                while (lastBid != -1 && batchScheduleMap.ContainsKey(lastBid))
                {
                    DeterministicBatchSchedule schedule = batchScheduleMap[lastBid];
                    inBatchTransactionCompletionMap.Remove(lastBid);
                    batchScheduleMap.Remove(lastBid);
                    lastBid = schedule.lastBatchID;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"\n Exception(ackBatchCommit):: exception {e.Message}");
            }

            /*
            try
            {
                if (bid == -1) return;
                //When called by the coordinator of a non-det transaction, batch bid may not access this grain.
                bool found = scheduleInfo.nodes.ContainsKey(bid);
                if (!found)
                {
                    // TODO: traverse from the tail, might have some holes (Yijian)
                    var node = scheduleInfo.tail;
                    while (node != null && node.id != -1)
                    {
                        if (node.isDet == false || node.id > bid)
                        {
                            node = node.prev;
                        }
                        else
                        {
                            // isDet == true && -1 < id < bid
                            if (node.commitmentPromise.Task.IsCompleted == false)
                            {
                                bid = node.id;
                                found = true;
                                break;
                            }
                            else
                            {
                                // if commitment promise is set true
                                return;
                            }
                        }
                    }
                    if (node == null || node.id == -1) return;
                }
                // case 1: the batch bid is in Nodes (found = true)
                // case 2: find the det node whose id < bid
                // if the comittment promise is already set true, it will just return
                // because it means there's another thread before this one trying to delete old nodes
                if (scheduleInfo.nodes[bid].commitmentPromise.Task.IsCompleted)
                {
                    Debug.Assert(found);
                    return;
                }

                scheduleInfo.nodes[bid].commitmentPromise.SetResult(true);
                scheduleInfo.removePreviousNodes(bid);    // TODO: remove all nodes before bid, not including bid, why (Yijian)
                var lastBid = batchScheduleMap[bid].lastBatchID;
                // if not contains the key lastBid, it means another thread before this one has removed this batch
                while (lastBid != -1 && batchScheduleMap.ContainsKey(lastBid))
                {
                    DeterministicBatchSchedule schedule = batchScheduleMap[lastBid];
                    inBatchTransactionCompletionMap.Remove(lastBid);
                    batchScheduleMap.Remove(lastBid);
                    lastBid = schedule.lastBatchID;
                }
            }
            catch(Exception e)
            {
                Console.WriteLine($"\n Exception(ackBatchCommit):: exception {e.Message}");
            }*/

        }

        public HashSet<int> getBeforeSet(int tid, out int maxBeforeBid)
        {
            return scheduleInfo.getBeforeSet(tid, out maxBeforeBid);
        }

        public HashSet<int> getAfterSet(int tid, int maxBeforeBid, out int minAfterBid)
        {
            return scheduleInfo.getAfterSet(tid, maxBeforeBid, out minAfterBid);
        }
    }


}
