using Orleans;
using Concurrency.Interface;
using System;
using System.Collections.Generic;
using System.Text;
using Utilities;
using System.Threading.Tasks;
using Concurrency.Interface.Logging;
using System.Timers;
using System.Diagnostics;

namespace Concurrency.Implementation
{
    public class GlobalTransactionCoordinator : Grain, IGlobalTransactionCoordinator

    {
        private int curTransactionID { get; set; }
        protected Guid myPrimaryKey;
        private IGlobalTransactionCoordinator neighbour;
        private List<IGlobalTransactionCoordinator> coordinatorList;

        //Timer
        private IDisposable disposable;
        private TimeSpan waitingTime = TimeSpan.FromSeconds(2);
        private TimeSpan batchInterval = TimeSpan.FromMilliseconds(1000);

        //Batch Schedule
        private Dictionary<int, Dictionary<Guid, DeterministicBatchSchedule>> batchSchedulePerGrain;
        private Dictionary<int, Dictionary<Guid, String>> batchGrainClassName;
        private SortedSet<int> batchesWaitingForCommit;
       

        //Maintains the status of batch processing
        private Dictionary<int, TaskCompletionSource<Boolean>> batchStatusMap;
        private int highestCommittedBatchID = -1;

        //Maintains the number of uncompleted grains of each batch
        private Dictionary<int, int> expectedAcksPerBatch;

        //Information controlling the emitting of non-deterministic transactions
        private int curEmitSeq { get; set; }
        Dictionary<int, TaskCompletionSource<Boolean>> emitPromiseMap;
        Dictionary<int, int> nonDeterministicEmitSize;
        //List buffering the incoming deterministic transaction requests
        Dictionary<int, List<TransactionContext>> deterministicTransactionRequests;

        //Emitting status
        private Boolean isEmitTimerOn = false;
        private Boolean hasToken = false;
        private BatchToken token;

        private ILoggingProtocol<String> log;

        //For test only
        private uint myId, neighbourId;
        private Boolean spawned = false;

        public override Task OnActivateAsync()
        {
            curTransactionID = -1;
            batchSchedulePerGrain = new Dictionary<int, Dictionary<Guid, DeterministicBatchSchedule>>();
            batchGrainClassName = new Dictionary<int, Dictionary<Guid, String>>();
            
            //actorLastBatch = new Dictionary<IDTransactionGrain, int>();
            expectedAcksPerBatch = new Dictionary<int, int>();
            batchStatusMap = new Dictionary<int, TaskCompletionSource<Boolean>>();
            myPrimaryKey = this.GetPrimaryKey();

            //Enable the following line for log
            //log = new Simple2PCLoggingProtocol<String>(this.GetType().ToString(), myPrimaryKey);
            disposable = RegisterTimer(EmitTransaction, null, waitingTime, batchInterval);

            this.batchesWaitingForCommit = new SortedSet<int>();
            curEmitSeq = 0;
            emitPromiseMap = new Dictionary<int, TaskCompletionSource<bool>>(); 
            nonDeterministicEmitSize = new Dictionary<int, int>();
            deterministicTransactionRequests = new Dictionary<int, List<TransactionContext>>();

            return base.OnActivateAsync();
        }

        /**
         *Client calls this function to submit deterministic transaction
         */
        public async Task<TransactionContext> NewTransaction(Dictionary<Guid, Tuple<string, int>> grainAccessInformation)
        {

            int myEmitSeq = this.curEmitSeq;
            if (deterministicTransactionRequests.ContainsKey(myEmitSeq) == false)
                deterministicTransactionRequests.Add(myEmitSeq, new List<TransactionContext>());

            TransactionContext context = new TransactionContext(grainAccessInformation);
            deterministicTransactionRequests[myEmitSeq].Add(context);
            
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
            return context;
        }


        /**
         *Client calls this function to submit non-deterministic transaction
         */
        public async Task<TransactionContext> NewTransaction()
        {
            TransactionContext context = null;
            try
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
                context = new TransactionContext(tid, myPrimaryKey);

                //Check if the emitting of the current non-deterministic batch is completed, if so, 
                //set the token promise and increment the curNondeterministicBatchID.
                nonDeterministicEmitSize[myEmitSeq] = nonDeterministicEmitSize[myEmitSeq] - 1;
                if (nonDeterministicEmitSize[myEmitSeq] == 0)
                {

                    nonDeterministicEmitSize.Remove(myEmitSeq);
                    emitPromiseMap.Remove(myEmitSeq);
                }
                Console.WriteLine($"Coordinator {myId}: emitted non-det transaction {tid}");
                
            } catch (Exception e)
            {
                Console.WriteLine($"Exception :: Coordinator {myId}: receives new non deterministic transaction {e.Message}");
            }
            return context;
        }

        /**
         *Coordinator calls this function to pass token
         */
        public async Task PassToken(BatchToken token)
        {
            //Console.WriteLine($"Coordinator {myId}: receives new token, batch ID {token.lastBatchID}, transaction ID {token.lastTransactionID}");
            this.hasToken = true;
            this.token = token;
            await EmitTransaction(token);             
        }

        /**
         *This functions is called when a new token is received or the timer is set
         */
        async Task EmitTransaction(Object obj)
        {

            if(obj == null)
            {
                this.isEmitTimerOn = true;
                if (this.hasToken == false)
                    return;
            }
            else if(! this.isEmitTimerOn)
            {
                this.hasToken = true;
                this.token = (BatchToken)obj;
                return;
            }

            int myCurEmitSeq = this.curEmitSeq;
            await EmitBatch(token);

            if (nonDeterministicEmitSize.ContainsKey(myCurEmitSeq))
            {                
                curTransactionID = token.lastTransactionID + 1;
                token.lastTransactionID = this.curTransactionID + nonDeterministicEmitSize[myCurEmitSeq] - 1;
            }

            //Console.WriteLine($"Coordinator {myId}: pass token to coordinator {neighbourId}");
            neighbour.PassToken(token);

            if (emitPromiseMap.ContainsKey(myCurEmitSeq))
            {
                emitPromiseMap[myCurEmitSeq].SetResult(true);
            }

            this.isEmitTimerOn = false;
            this.hasToken = false;
            this.token = null;
        }

        /**
         *This functions is called to emit batch of deterministic transactions
         */
        async Task EmitBatch(BatchToken token)
        {
            int myEmitSequence = this.curEmitSeq;

            List<TransactionContext> transactionList;
            Boolean shouldEmit = deterministicTransactionRequests.TryGetValue(myEmitSequence, out transactionList);

            //Return if there is no deterministic transactions waiting for emit
            if (shouldEmit == false)
                return;

            curEmitSeq++;
            curTransactionID = token.lastTransactionID + 1;
            int curBatchID = curTransactionID;

            foreach (TransactionContext context in transactionList)
            {
                context.batchID = curBatchID;
                context.transactionID = curTransactionID++;

                //update batch schedule
                if (batchSchedulePerGrain.ContainsKey(context.batchID) == false)
                {
                    batchSchedulePerGrain.Add(context.batchID, new Dictionary<Guid, DeterministicBatchSchedule>());
                    batchGrainClassName.Add(context.batchID, new Dictionary<Guid, String>());
                }

                //update the schedule for each grain accessed by this transaction
                Dictionary<Guid, DeterministicBatchSchedule> grainSchedule = batchSchedulePerGrain[context.batchID];

                foreach (var item in context.grainAccessInformation)
                {
                    batchGrainClassName[context.batchID][item.Key] = item.Value.Item1;
                    if (grainSchedule.ContainsKey(item.Key) == false)
                        grainSchedule.Add(item.Key, new DeterministicBatchSchedule(context.batchID));
                    grainSchedule[item.Key].AddNewTransaction(context.transactionID, item.Value.Item2);

                }
                context.grainAccessInformation.Clear();
            }

            Dictionary<Guid, DeterministicBatchSchedule> curScheduleMap = batchSchedulePerGrain[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, curScheduleMap.Count);

            //update thelast batch ID for each grain accessed by this batch
            foreach(var item in curScheduleMap)
            {
                Guid grain = item.Key;
                DeterministicBatchSchedule schedule = item.Value;
                if (token.lastBatchPerGrain.ContainsKey(grain))
                    schedule.lastBatchID = token.lastBatchPerGrain[grain];
                else
                    schedule.lastBatchID = -1;
                Debug.Assert(schedule.batchID > schedule.lastBatchID);
                token.lastBatchPerGrain[grain] = schedule.batchID;
            }
            token.lastBatchID = curBatchID;
            token.lastTransactionID = this.curTransactionID - 1;

            //garbage collection
            if(this.highestCommittedBatchID > token.highestCommittedBatchID)
            {
                List<Guid> expiredGrains = new List<Guid>();
                foreach (var item in token.lastBatchPerGrain)
                {
                    if (item.Value <= this.highestCommittedBatchID)
                        expiredGrains.Add(item.Key);
                }
                foreach (var item in expiredGrains)
                    token.lastBatchPerGrain.Remove(item);
                token.highestCommittedBatchID = this.highestCommittedBatchID;
            }


            if (batchStatusMap.ContainsKey(curBatchID) == false)
                batchStatusMap.Add(curBatchID, new TaskCompletionSource<Boolean>());

            var v = typeof(IDeterministicTransactionCoordinator);
            if (log != null)
            {
                HashSet<Guid> participants = new HashSet<Guid>();
                participants.UnionWith(curScheduleMap.Keys);
                await log.HandleOnPrepareInDeterministicProtocol(curBatchID, participants);
            }

            foreach (KeyValuePair<Guid, DeterministicBatchSchedule> item in curScheduleMap)
            {
                var dest = this.GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, batchGrainClassName[curBatchID][item.Key]);
                DeterministicBatchSchedule schedule = item.Value;
                schedule.globalCoordinator = this.myPrimaryKey;
                Task emit = dest.ReceiveBatchSchedule(schedule);
            }
            batchGrainClassName.Remove(curBatchID);
            //Console.WriteLine($"\n Coordinator {this.myId}: sent schedule for batch {curBatchID}, which contains {transactionList.Count} transactions.");
        }

        //Grain calls this function to ack its completion of a batch execution
        public async Task AckBatchCompletion(int bid, Guid executor_id)
        {
            //Console.WriteLine($"\n Coordinator: {myId} receives completion ack for batch {bid} from {executor_id}, expecting {expectedAcksPerBatch[bid]} acks. {batchSchedulePerGrain[bid].ContainsKey(executor_id)}");
            Debug.Assert(expectedAcksPerBatch.ContainsKey(bid) && batchSchedulePerGrain[bid].ContainsKey(executor_id));
            if (expectedAcksPerBatch.ContainsKey(bid) && batchSchedulePerGrain[bid].ContainsKey(executor_id))
            {
                expectedAcksPerBatch[bid]--;
                if (expectedAcksPerBatch[bid] == 0)
                {
                    if(this.highestCommittedBatchID == bid - 1)
                    {
                        //commit
                        this.highestCommittedBatchID = bid;
                        if (log != null)
                            await log.HandleOnCommitInDeterministicProtocol(bid);
                        await BroadcastCommit();
                    }
                    else
                    {
                        //Put this batch into a list waiting for commit
                        this.batchesWaitingForCommit.Add(bid);
                    }

                    expectedAcksPerBatch.Remove(bid);
                    batchSchedulePerGrain.Remove(bid);
                    batchGrainClassName.Remove(bid);

                    batchStatusMap[bid].SetResult(true);
                    Console.WriteLine($"\n Coordinator: batch {bid} has been committed.");
                }
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

        private async Task BroadcastCommit()
        {

            List<Task> tasks = new List<Task>();
            foreach (var coordinator in this.coordinatorList)
            {
                tasks.Add(coordinator.NotifyCommit(this.highestCommittedBatchID));
            }
            await Task.WhenAll(tasks);
        }

        public async Task NotifyCommit(int bid)
        {
            if (bid > this.highestCommittedBatchID)
                this.highestCommittedBatchID = bid;

            Boolean commitOccur = false;
            
            while (this.batchesWaitingForCommit.Count != 0 && batchesWaitingForCommit.Min == highestCommittedBatchID + 1)
            {
                //commit
                this.highestCommittedBatchID++;
                batchesWaitingForCommit.Remove(batchesWaitingForCommit.Min);
                commitOccur = true;
            }
            
            if (commitOccur == true)
            {
                if (log != null)
                    await log.HandleOnCommitInDeterministicProtocol(this.highestCommittedBatchID);
                await BroadcastCommit();
            }
            //Console.WriteLine($"\n Coordinator {this.myId} finished processing commit notification for batch {bid}");
            
        }

        public Task<HashSet<int>> GetCompleteAfterSet(int tid, Dictionary<int, String> grains)
        {
            return null;


        }

        public async Task SpawnCoordinator(uint myId, uint numofCoordinators)
        {
            if (this.spawned)
                return;
            else
                this.spawned = true;

            uint neighbourId = (myId + 1) % numofCoordinators;
            neighbour = this.GrainFactory.GetGrain<IGlobalTransactionCoordinator>(Helper.convertUInt32ToGuid(neighbourId));
            coordinatorList = new List<IGlobalTransactionCoordinator>();
            for(uint i=0; i<numofCoordinators; i++)
            {
                if (i != myId)
                    coordinatorList.Add(this.GrainFactory.GetGrain<IGlobalTransactionCoordinator>(Helper.convertUInt32ToGuid(i)));
            }
            //The "first" coordinator starts the token passing
            if (myId == 0)
            {
                System.Threading.Thread.Sleep(2000);
                BatchToken token = new BatchToken(-1, -1);
                PassToken(token);
            }
            this.myId = myId;
            this.neighbourId = neighbourId;
            Console.WriteLine($"\n Coordinator {myId}: is initialized, my next neighbour is coordinator {neighbourId}");
        }
    }
}
