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
            }

            //Promise that the current execution should wait for
            var promise = new TaskCompletionSource<Boolean>();
            inBatchTransactionCompletionMap[bid][tid].Add(promise);

            int nextTid;
            ////Check if this call can be executed;
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
                    promise.SetResult(true);
                }

                if (promise.Task.IsCompleted == false)
                    await promise.Task;
            }
        }

        //For nonDeterninistic Transactions
        public void waitForTurn(int tid)
        {

        }

        //For deterministic transaction
        public void ackComplete(int bid, int tid)
        {

        }

        //
        public void ackComplete(int tid)
        {

        }

    }


}
