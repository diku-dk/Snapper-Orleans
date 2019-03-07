using Concurrency.Utilities;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation
{


    class TransactionScheduler
    {

        //public INondeterministicTransactionCoordinator ndtc;
        //public IDeterministicTransactionCoordinator dtc;
        //private int lastBatchId;
        private Dictionary<int, DeterministicBatchSchedule> batchScheduleMap;
        //private Dictionary<long, Guid> coordinatorMap;
        //private Queue<DeterministicBatchSchedule> batchScheduleQueue;
        private NonDeterministicSchedule nonDetSchedule;
        private Dictionary<int, TaskCompletionSource<Boolean>> batchCompletionMap;
        private TaskCompletionSource<Boolean> nonDetCompletion;
        private Dictionary<int, Dictionary<int, List<TaskCompletionSource<Boolean>>>> inBatchTransactionCompletionMap;


        //for deterministic transactions
        public async void waitForTurn(int bid, int tid)
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

            //Promise created for the current execution, which is waited by the next function call
            var myPromise = new TaskCompletionSource<Boolean>();
            inBatchTransactionCompletionMap[bid][tid].Add(myPromise);

            int nextTid;

            //Check if this call can be executed;
            if (batchScheduleMap.ContainsKey(bid))
            {
                DeterministicBatchSchedule schedule = batchScheduleMap[bid];

                if(nonDetCompletion.Task.IsCompleted == false || batchCompletionMap[schedule.lastBatchID].Task.IsCompleted == false)
                {
                    //If it is not the trun fir this batch, then await or its turn
                    await nonDetCompletion.Task;
                    await batchCompletionMap[schedule.lastBatchID].Task;
                }
                //Check if this transaction cen be executed
                nextTid = batchScheduleMap[bid].curExecTransaction();
                if (tid == nextTid)
                {
                    //Console.WriteLine($"\n\n{this.GetType()}: Set Promise for Tx: {tid} in batch {bid} within Execute(). \n\n");

                    //Set the promise for the first function call of this Transaction if it is not
                    if (inBatchTransactionCompletionMap[bid][tid][0].Task.IsCompleted == false)
                        inBatchTransactionCompletionMap[bid][tid][0].SetResult(true);
                }
            }

            if (lastPromise.Task.IsCompleted == false)
                await lastPromise.Task;
        }

        //For nonDeterninistic Transactions
        public void waitForTurn(int tid)
        {

        }

        //For deterministic transaction
        public void ackComplete(int bid, int tid)
        {
            DeterministicBatchSchedule schedule = batchScheduleMap[bid];
            schedule.AccessIncrement(tid);

            //Record the execution in batch schedule
            batchScheduleMap[bid].AccessIncrement(tid);

            //Find the next transaction to be executed in this batch;
            int nextTid = batchScheduleMap[bid].curExecTransaction();
            //Console.WriteLine($"\n\n{this.GetType()}: nextTid is {nextTid} \n\n");
            if (nextTid != -1)
            {
                if (inBatchTransactionCompletionMap[bid].ContainsKey(nextTid) && inBatchTransactionCompletionMap[bid][nextTid].Count > 0)
                {
                    //Console.WriteLine($"\n\n{this.GetType()}: Set promise result for Tx {nextTid} \n\n");
                    inBatchTransactionCompletionMap[bid][nextTid][0].SetResult(true);
                }
            }
            else
            {
                //batchScheduleQueue.Dequeue();                
                //Log the state now
                if (log != null && state != null)
                    await log.HandleOnCompleteInDeterministicProtocol(state, bid, batchScheduleMap[bid].globalCoordinator);

                var batchCoordinator = this.GrainFactory.GetGrain<IGlobalTransactionCoordinator>(batchScheduleMap[bid].globalCoordinator);
                Task ack = batchCoordinator.AckBatchCompletion(bid, myPrimaryKey);
                batchScheduleMap.Remove(bid);

                //The schedule for this batch {$bid} has been completely executed. Check if any promise for next batch can be set.
                this.batchCompletionMap[bid].SetResult(true);


                if (batchScheduleQueue.Count != 0)
                {
                    DeterministicBatchSchedule nextSchedule = batchScheduleQueue.Peek();
                    nextTid = nextSchedule.curExecTransaction();
                    if (promiseMap.ContainsKey(nextTid) && promiseMap[nextTid].Count != 0)
                    {
                        promiseMap[nextTid][0].SetResult(true);
                    }
                }

            }

        }

        //
        public void ackComplete(int tid)
        {

        }

    }


}
