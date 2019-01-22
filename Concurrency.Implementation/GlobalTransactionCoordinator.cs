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

        //Timer
        private IDisposable disposable;
        private TimeSpan waitingTime = TimeSpan.FromSeconds(1);
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

        //Emitting status
        private Boolean isEmitTimerOn = false;
        private Boolean hasToken = false;

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
            return base.OnActivateAsync();
        }

        public Task<TransactionContext> NewTransaction(Dictionary<Guid, Tuple<string, int>> grainAccessInformation)
        {
            throw new NotImplementedException();
        }

        public Task<TransactionContext> NewTransaction()
        {
            throw new NotImplementedException();
        }

        public async Task PassToken(BatchToken token)
        {
            this.hasToken = true;
            await EmitTransaction(token);

            //TODO: Pass the token to the next coordinator
        }

        async Task EmitTransaction(Object obj)
        {
            if (hasToken == false && obj == null)
            {
                this.isEmitTimerOn = true;
                return;
            }
            else if(this.isEmitTimerOn == false)
            {
                return;
            }

            BatchToken token = (BatchToken) obj;
            await EmitBatch(token);

            ResetEmitStatus();
        }

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

        async Task EmitNondeterministicTransaction()
        {

        }

        public Task AckTransactionCompletion(int bid, Guid executor_id)
        {
            throw new NotImplementedException();
        }


        private void ResetEmitStatus()
        {
            this.isEmitTimerOn = false;
            this.hasToken = false;
        }


    }
}
