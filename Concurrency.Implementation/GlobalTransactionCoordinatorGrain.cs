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
using System.Threading;

namespace Concurrency.Implementation
{
    public class GlobalTransactionCoordinatoGrain : Grain, IGlobalTransactionCoordinatorGrain

    {
        protected Guid myPrimaryKey;
        private IGlobalTransactionCoordinatorGrain neighbour;
        private List<IGlobalTransactionCoordinatorGrain> coordinatorList;

        //Timer
        private IDisposable disposable;
        private int batchIntervalMSecs;
        private int backoffTimeIntervalMSecs;
        private int idleIntervalTillBackOffSecs;
        private TimeSpan waitingTime;
        private TimeSpan batchInterval;
        private float smoothingPreAllocationFactor = 0.5f;

        //Batch Schedule
        // only include batches that are emitted from this coordinator
        private Dictionary<int, Dictionary<Guid, DeterministicBatchSchedule>> batchSchedulePerGrain;
        private Dictionary<int, Dictionary<Guid, String>> batchGrainClassName;
        private SortedSet<int> batchesWaitingForCommit;


        //Maintains the status of batch processing
        private Dictionary<int, int> lastBatchIDMap;   // the last emitted batch before this batch among all coordinators
        private Dictionary<int, TaskCompletionSource<Boolean>> batchStatusMap;
        private int highestCommittedBatchID = -1;

        //Maintains the number of uncompleted grains of each batch
        private Dictionary<int, int> expectedAcksPerBatch;

        //Information controlling the emitting of non-deterministic transactions
        private int detEmitSeq, nonDetEmitSeq;
        Dictionary<int, TaskCompletionSource<Boolean>> detEmitPromiseMap, nonDetEmitPromiseMap;
        Dictionary<int, int> nonDetEmitID;
        Dictionary<int, int> nonDeterministicEmitSize;
        //List buffering the incoming deterministic transaction requests
        Dictionary<int, List<TransactionContext>> deterministicTransactionRequests;
        private int numTransactionIdsPreAllocated;
        private int numTransactionIdsReserved;
        private int tidToAllocate;

        //Emitting status
        private Boolean isEmitTimerOn = false;

        // TODO: log is not initialized (Yijian)
        private ILoggingProtocol<String> log;

        //For test only
        private uint myId, neighbourId;
        private Boolean spawned = false;

        //disable token
        BatchToken token;

        public override Task OnActivateAsync()
        {
            token = new BatchToken(-1, -1);
            batchSchedulePerGrain = new Dictionary<int, Dictionary<Guid, DeterministicBatchSchedule>>();
            batchGrainClassName = new Dictionary<int, Dictionary<Guid, String>>();
            lastBatchIDMap = new Dictionary<int, int>();
            //actorLastBatch = new Dictionary<IDTransactionGrain, int>();
            expectedAcksPerBatch = new Dictionary<int, int>();
            batchStatusMap = new Dictionary<int, TaskCompletionSource<Boolean>>();
            myPrimaryKey = this.GetPrimaryKey();

            //Enable the following line for log
            //log = new Simple2PCLoggingProtocol<String>(this.GetType().ToString(), myPrimaryKey);
            this.batchesWaitingForCommit = new SortedSet<int>();
            detEmitSeq = 0;
            nonDetEmitSeq = 0;
            numTransactionIdsPreAllocated = 0;
            numTransactionIdsReserved = 0;
            tidToAllocate = -1;
            detEmitPromiseMap = new Dictionary<int, TaskCompletionSource<bool>>();
            nonDetEmitPromiseMap = new Dictionary<int, TaskCompletionSource<bool>>();
            nonDeterministicEmitSize = new Dictionary<int, int>();
            nonDetEmitID = new Dictionary<int, int>();
            deterministicTransactionRequests = new Dictionary<int, List<TransactionContext>>();

            return base.OnActivateAsync();
        }

        /**
         *Client calls this function to submit deterministic transaction
         */
        public async Task<TransactionContext> NewTransaction(Dictionary<Guid, Tuple<string, int>> grainAccessInformation)
        {
            int myEmitSeq = this.detEmitSeq;
            if (deterministicTransactionRequests.ContainsKey(myEmitSeq) == false)
                deterministicTransactionRequests.Add(myEmitSeq, new List<TransactionContext>());

            TransactionContext context = new TransactionContext(grainAccessInformation);
            deterministicTransactionRequests[myEmitSeq].Add(context);
            
            TaskCompletionSource<bool> emitting;
            if (!detEmitPromiseMap.ContainsKey(myEmitSeq))
            {
                detEmitPromiseMap.Add(myEmitSeq, new TaskCompletionSource<bool>());
            }
            emitting = detEmitPromiseMap[myEmitSeq];
            if (emitting.Task.IsCompleted != true)
            {
                await emitting.Task;
            }
            //Console.WriteLine($"Coordinator {myId}: emitted deterministic transaction {context.transactionID}");
            context.highestBatchIdCommitted = this.highestCommittedBatchID;
            return context;
        }


        /**
         *Client calls this function to submit non-deterministic transaction
         */
        public async Task<TransactionContext> NewTransaction()
        {
            //Console.WriteLine($"Coordinator {myId}: received non-det transaction");
            if (numTransactionIdsReserved-- > 0)
            {
                Debug.Assert(tidToAllocate != 0);
                //Console.WriteLine($"Coordinator {myId}: emitted non-det transaction with pre-allocated tid {tidToAllocate}");
                var ctx = new TransactionContext(tidToAllocate++);
                ctx.highestBatchIdCommitted = this.highestCommittedBatchID;
                return ctx;
            }
            TransactionContext context = null;
            try
            {
                TaskCompletionSource<bool> emitting;
                int myEmitSeq = this.nonDetEmitSeq;
                if (!nonDetEmitPromiseMap.ContainsKey(myEmitSeq))
                {
                    nonDetEmitPromiseMap.Add(myEmitSeq, new TaskCompletionSource<bool>());
                    nonDeterministicEmitSize.Add(myEmitSeq, 0);
                }
                emitting = nonDetEmitPromiseMap[myEmitSeq];
                nonDeterministicEmitSize[myEmitSeq] = nonDeterministicEmitSize[myEmitSeq] + 1;

                if (emitting.Task.IsCompleted != true)
                {                    
                    await emitting.Task;
                }
                int tid = nonDetEmitID[myEmitSeq]++;
                context = new TransactionContext(tid);

                //Check if the emitting of the current non-deterministic batch is completed, if so, 
                //set the token promise and increment the curNondeterministicBatchID.
                nonDeterministicEmitSize[myEmitSeq] = nonDeterministicEmitSize[myEmitSeq] - 1;
                if (nonDeterministicEmitSize[myEmitSeq] == 0)
                {
                    nonDeterministicEmitSize.Remove(myEmitSeq);
                    nonDetEmitID.Remove(myEmitSeq);
                }
                //Console.WriteLine($"Coordinator {myId}: emitted non-det transaction {tid}");
                
            } catch (Exception e)
            {
                //Console.WriteLine($"Exception :: Coordinator {myId}: receives new non deterministic transaction {e.Message}");
            }
            context.highestBatchIdCommitted = this.highestCommittedBatchID;
            return context;
        }


        public async Task CheckBackoff(BatchToken token)
        {
            if(detEmitPromiseMap.Count == 0 && nonDetEmitPromiseMap.Count == 0)
            {
                //Console.WriteLine($"Coordinator {myId}: backs off,  NO request.");
                //The coordinator has no transaction request
                if (token.backoff)
                {
                    //Block
                    await Task.Delay(TimeSpan.FromMilliseconds(backoffTimeIntervalMSecs / (coordinatorList.Count+1)));
                }
                else if (!token.idleToken)
                {
                    token.idleToken = true;
                    token.markedIdleByCoordinator = myPrimaryKey;
                    var curTime = DateTime.Now;
                    token.backOffProbeStartTime = curTime.Hour * 3600 + curTime.Minute * 60 + curTime.Second;
                } else if(token.markedIdleByCoordinator == myPrimaryKey)                {
                    var curTime = DateTime.Now;
                    var curTimeInSecs = curTime.Hour * 3600 + curTime.Minute * 60 + curTime.Second;                    
                    if(curTimeInSecs - token.backOffProbeStartTime > this.idleIntervalTillBackOffSecs)
                    {
                        //Token traverses full round being idle, enable backoff
                        token.backoff = true;
                        await Task.Delay(TimeSpan.FromMilliseconds(backoffTimeIntervalMSecs / (coordinatorList.Count+1)));
                    }                        
                }
            } else
            {
                //Console.WriteLine($"Coordinator {myId}: stops backing off.");
                //The coordinator has  transaction requests
                if (token.backoff)
                {
                    token.backoff = false;
                } else if(token.idleToken)
                {
                    token.idleToken = false;
                }
            }
        }

        /**
         *Coordinator calls this function to pass token
         */
        public async Task PassToken(BatchToken token)
        {
            //Console.WriteLine($"Coordinator {myId}: receives new token at {DateTime.Now.TimeOfDay.ToString()}");
            numTransactionIdsReserved = 0; //Reset the range of pre-allocation
            await CheckBackoff(token);
            await EmitTransaction(token);
            tidToAllocate = token.lastTransactionID + 1;
            token.lastTransactionID += numTransactionIdsPreAllocated;
            //Console.WriteLine($"Coordinator {myId} before {tidToAllocate} after {token.lastTransactionID}");
            neighbour.PassToken(token);
            //Console.WriteLine($"Coordinator {myId} passed token to {this.neighbour.GetPrimaryKey()}.");
        }

        /**
         *This functions is called when a new token is received or the timer is set
         */
        async Task EmitTransaction(Object obj)
        {
            numTransactionIdsReserved = 0;
            EmitNonDeterministicTransactions();
            tidToAllocate = token.lastTransactionID + 1;
            token.lastTransactionID += numTransactionIdsPreAllocated;
            /*
            BatchToken token;
            //The timer expires
            if (obj == null)
            {
                this.isEmitTimerOn = true;
                return;
            }

            //The token arrives, but the timer is not expired yet
            if(! this.isEmitTimerOn)
            {
                //Emit non-det transactions and release token
                token = (BatchToken)obj;
                EmitNonDeterministicTransactions(token);
                return;
            }

            token = (BatchToken)obj;
            await EmitDeterministicTransactions(token);
            EmitNonDeterministicTransactions(token);
            this.isEmitTimerOn = false;*/
        }

        // private void EmitNonDeterministicTransactions(BatchToken token)
        private void EmitNonDeterministicTransactions()
        {
            int myEmitSequence = this.nonDetEmitSeq;
            if (nonDeterministicEmitSize.ContainsKey(myEmitSequence))
            {
                //curTransactionID = token.lastTransactionID + 1;
                //Estimate a pre-allocation size based on moving average
                var waitingTxns = nonDeterministicEmitSize[myEmitSequence];                
                numTransactionIdsPreAllocated = (int)(smoothingPreAllocationFactor * (float)waitingTxns + (1 - smoothingPreAllocationFactor) * (float)numTransactionIdsPreAllocated);
                numTransactionIdsReserved = numTransactionIdsPreAllocated;
                //Console.WriteLine($"Coordinator id {myId} waitingtxns {waitingTxns}, preallocatedTransactions {numTransactionIdsPreAllocated}");
                Debug.Assert(nonDetEmitID.ContainsKey(myEmitSequence) == false);
                nonDetEmitID.Add(myEmitSequence, token.lastTransactionID + 1);
                token.lastTransactionID += nonDeterministicEmitSize[myEmitSequence];
                this.nonDetEmitSeq++;
                nonDetEmitPromiseMap[myEmitSequence].SetResult(true);
                nonDetEmitPromiseMap.Remove(myEmitSequence);
            } 
            else
            {
                numTransactionIdsPreAllocated = 0;
                numTransactionIdsReserved = 0;
            }
        }
        /**
         *This functions is called to emit batch of deterministic transactions
         */
        async Task EmitDeterministicTransactions(BatchToken token)
        {
            int myEmitSequence = this.detEmitSeq;
            List<TransactionContext> transactionList;
            Boolean shouldEmit = deterministicTransactionRequests.TryGetValue(myEmitSequence, out transactionList);

            //Return if there is no deterministic transactions waiting for emit
            if (shouldEmit == false)
                return;
            this.detEmitSeq++;
            int curBatchID = token.lastTransactionID + 1;

            foreach (TransactionContext context in transactionList)
            {
                context.batchID = curBatchID;
                context.transactionID = ++token.lastTransactionID;
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

            //update the last batch ID for each grain accessed by this batch
            foreach (var item in curScheduleMap)
            {
                Guid grain = item.Key;
                DeterministicBatchSchedule schedule = item.Value;
                if (token.lastBatchPerGrain.ContainsKey(grain))
                    schedule.lastBatchID = token.lastBatchPerGrain[grain];
                else
                    schedule.lastBatchID = -1;
                schedule.globalCoordinator = this.myPrimaryKey;
                Debug.Assert(schedule.batchID > schedule.lastBatchID);
                token.lastBatchPerGrain[grain] = schedule.batchID;
            }
            this.lastBatchIDMap.Add(curBatchID, token.lastBatchID);
            token.lastBatchID = curBatchID;
            //garbage collection
            if (this.highestCommittedBatchID > token.highestCommittedBatchID)
            {
                List<Guid> expiredGrains = new List<Guid>();
                foreach (var item in token.lastBatchPerGrain)
                {
                    // only when last batch is already committed, the next emmitted batch can have its lastBid = -1 again
                    if (item.Value <= this.highestCommittedBatchID)
                        expiredGrains.Add(item.Key);
                }
                foreach (var item in expiredGrains)
                    token.lastBatchPerGrain.Remove(item);
                token.highestCommittedBatchID = this.highestCommittedBatchID;
            }

            if (!batchStatusMap.ContainsKey(curBatchID))
                batchStatusMap.Add(curBatchID, new TaskCompletionSource<Boolean>());

            //var v = typeof(IDeterministicTransactionCoordinator);
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
                schedule.highestCommittedBatchId = this.highestCommittedBatchID;
                Task emit = dest.ReceiveBatchSchedule(schedule);
            }
            batchGrainClassName.Remove(curBatchID);
            this.detEmitPromiseMap[myEmitSequence].SetResult(true);
            this.deterministicTransactionRequests.Remove(myEmitSequence);
            this.detEmitPromiseMap.Remove(myEmitSequence);
        }

        /*
         * Grain calls this function to ack its completion of a batch execution
        */
        public async Task AckBatchCompletion(int bid, Guid executor_id)
        {
            //Console.WriteLine($"\n Coordinator: {myId} receives completion ack for batch {bid} from {executor_id}, expecting {expectedAcksPerBatch[bid]} acks. {batchSchedulePerGrain[bid].ContainsKey(executor_id)}");
            Debug.Assert(expectedAcksPerBatch.ContainsKey(bid) && batchSchedulePerGrain[bid].ContainsKey(executor_id));
            expectedAcksPerBatch[bid]--;
            if (expectedAcksPerBatch[bid] == 0)
            {
                Debug.Assert(lastBatchIDMap.ContainsKey(bid));
                if (lastBatchIDMap[bid] == highestCommittedBatchID)
                {
                    //commit
                    this.highestCommittedBatchID = bid;
                    CleanUp(bid);
                    if (log != null)
                        await log.HandleOnCommitInDeterministicProtocol(bid);
                    await BroadcastCommit();
                }
                else
                {
                    //Put this batch into a list waiting for commit
                    this.batchesWaitingForCommit.Add(bid);
                }

            }
        }

        public void CleanUp(int bid)
        {
            expectedAcksPerBatch.Remove(bid);
            batchSchedulePerGrain.Remove(bid);
            batchGrainClassName.Remove(bid);
            lastBatchIDMap.Remove(bid);
            batchStatusMap[bid].SetResult(true);
            //Console.WriteLine($"\n Coordinator {this.myId}: batch {bid} has been committed, my highest committed batch id is {this.highestCommittedBatchID}");
        }

        public async Task<bool> checkBatchCompletion(int bid)
        {
            if (bid <= this.highestCommittedBatchID) return true;
            if (batchStatusMap.ContainsKey(bid) == false) batchStatusMap.Add(bid, new TaskCompletionSource<bool>());
            if (batchStatusMap[bid].Task.IsCompleted == false) await batchStatusMap[bid].Task;
            batchStatusMap.Remove(bid);
            return true;
        }

        private async Task BroadcastCommit()
        {
            //Console.WriteLine($"\n Commit batch {this.highestCommittedBatchID} \n");
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
            
            while (this.batchesWaitingForCommit.Count != 0 && lastBatchIDMap[batchesWaitingForCommit.Min] == highestCommittedBatchID)
            {
                //commit
                var commitBid = batchesWaitingForCommit.Min;
                this.highestCommittedBatchID = commitBid;
                CleanUp(commitBid);
                batchesWaitingForCommit.Remove(commitBid);
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

        public Task<HashSet<int>> GetCompleteAfterSet(Dictionary<Guid, int> grains, Dictionary<Guid, String> names)
        {
            return null;
        }

        public async Task SpawnCoordinator(uint myId, uint numofCoordinators, int batchIntervalMSecs, int backoffIntervalMSecs, int idleIntervalTillBackOffSecs)
        {
            if (this.spawned)
            {
                //No updated configuration for now so just ignore repeated spawn calls
                Console.WriteLine($"Coordinator {myId} receives spawn request but has already been spawned");
                return;
            } else
                this.spawned = true;
            
            waitingTime = TimeSpan.FromMilliseconds(1);            
            this.batchIntervalMSecs = batchIntervalMSecs;
            if(idleIntervalTillBackOffSecs > 3600 )
            {
                throw new Exception("Too high value for back off probing -> cannot exceed an 1 hour");
            }
            this.idleIntervalTillBackOffSecs = idleIntervalTillBackOffSecs;
            batchInterval = TimeSpan.FromMilliseconds(batchIntervalMSecs);            
            this.backoffTimeIntervalMSecs = backoffIntervalMSecs;
            

            uint neighbourId = (myId + 1) % numofCoordinators;
            neighbour = this.GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(Helper.convertUInt32ToGuid(neighbourId));
            coordinatorList = new List<IGlobalTransactionCoordinatorGrain>();
            for (uint i = 0; i < numofCoordinators; i++)
            {
                if (i != myId)
                    coordinatorList.Add(this.GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(Helper.convertUInt32ToGuid(i)));
            }            
            this.myId = myId;
            this.neighbourId = neighbourId;
            disposable = RegisterTimer(EmitTransaction, null, waitingTime, batchInterval);
            Console.WriteLine($"\n Coordinator {myId}: is initialized, my next neighbour is coordinator {neighbourId}");
        }
    }
}
