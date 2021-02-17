using System;
using Orleans;
using Utilities;
using System.Diagnostics;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Implementation.Logging;

namespace Concurrency.Implementation
{
    [CoordPlacementStrategy]
    public class GlobalTransactionCoordinatoGrain : Grain, IGlobalTransactionCoordinatorGrain
    {
        private int myID;
        private int neighborID;
        private string grainClassName;
        private int highestCommittedBatchID;
        private ILoggingProtocol<string> log;
        private int backoffTimeIntervalMSecs;
        private int detEmitSeq, nonDetEmitSeq;
        private int idleIntervalTillBackOffSecs;
        private float smoothingPreAllocationFactor;
        private Dictionary<int, int> lastBatchIDMap;       // <bid, lastBid>
        private Dictionary<int, int> coordEmitLastBatch;   // <bid, coordID who emit lastBid>
        private SortedList<int, DateTime> batchStartTime;
        private Dictionary<int, int> expectedAcksPerBatch;
        private List<IGlobalTransactionCoordinatorGrain> coordList;
        private Dictionary<int, TaskCompletionSource<bool>> batchesWaitingForCommit;
        Dictionary<int, TaskCompletionSource<bool>> detEmitPromiseMap, nonDetEmitPromiseMap;
        private Dictionary<int, Dictionary<int, DeterministicBatchSchedule>> batchSchedulePerGrain;

        Dictionary<int, int> nonDetEmitID;
        Dictionary<int, int> nonDeterministicEmitSize;

        // List buffering the incoming deterministic transaction requests
        Dictionary<int, List<TransactionContext>> deterministicTransactionRequests;
        private int numTransactionIdsPreAllocated;
        private int numTransactionIdsReserved;
        private int tidToAllocate;

        // if persist PACT input    <bid, <tid, <grainID, input>>>
        private Dictionary<int, Dictionary<int, Tuple<int, object>>> pactLogInfo = new Dictionary<int, Dictionary<int, Tuple<int, object>>>();

        public override Task OnActivateAsync()
        {
            detEmitSeq = 0;
            nonDetEmitSeq = 0;
            tidToAllocate = -1;
            highestCommittedBatchID = -1;
            numTransactionIdsReserved = 0;
            numTransactionIdsPreAllocated = 0;
            smoothingPreAllocationFactor = 0.5f;
            myID = (int)this.GetPrimaryKeyLong();
            nonDetEmitID = new Dictionary<int, int>();
            lastBatchIDMap = new Dictionary<int, int>();
            coordEmitLastBatch = new Dictionary<int, int>();
            batchStartTime = new SortedList<int, DateTime>();
            expectedAcksPerBatch = new Dictionary<int, int>();
            nonDeterministicEmitSize = new Dictionary<int, int>();
            coordList = new List<IGlobalTransactionCoordinatorGrain>();
            detEmitPromiseMap = new Dictionary<int, TaskCompletionSource<bool>>();
            nonDetEmitPromiseMap = new Dictionary<int, TaskCompletionSource<bool>>();
            batchesWaitingForCommit = new Dictionary<int, TaskCompletionSource<bool>>();
            deterministicTransactionRequests = new Dictionary<int, List<TransactionContext>>();
            batchSchedulePerGrain = new Dictionary<int, Dictionary<int, DeterministicBatchSchedule>>();
            return base.OnActivateAsync();
        }

        // if persist PACT input
        public async Task<TransactionContext> NewTransaction(Dictionary<int, int> grainAccessInformation, int grainID, object input)
        {
            var myEmitSeq = detEmitSeq;
            var context = new TransactionContext(grainAccessInformation);

            // if persist PACT input
            context.grainID = grainID;
            context.input = input;

            if (deterministicTransactionRequests.ContainsKey(myEmitSeq) == false)
            {
                deterministicTransactionRequests.Add(myEmitSeq, new List<TransactionContext>());
                detEmitPromiseMap.Add(myEmitSeq, new TaskCompletionSource<bool>());
                batchStartTime.Add(myEmitSeq, DateTime.Now);
            }
            deterministicTransactionRequests[myEmitSeq].Add(context);
            var emitting = detEmitPromiseMap[myEmitSeq].Task;
            if (emitting.IsCompleted != true) await emitting;
            context.highestBatchIdCommitted = highestCommittedBatchID;
            return context;
        }

        // for PACT
        public async Task<TransactionContext> NewTransaction(Dictionary<int, int> grainAccessInformation)
        {
            var myEmitSeq = detEmitSeq;
            var context = new TransactionContext(grainAccessInformation);
            if (deterministicTransactionRequests.ContainsKey(myEmitSeq) == false)
            {
                deterministicTransactionRequests.Add(myEmitSeq, new List<TransactionContext>());
                detEmitPromiseMap.Add(myEmitSeq, new TaskCompletionSource<bool>());
                batchStartTime.Add(myEmitSeq, DateTime.Now);
            }
            deterministicTransactionRequests[myEmitSeq].Add(context);
            var emitting = detEmitPromiseMap[myEmitSeq].Task;
            if (emitting.IsCompleted != true) await emitting;
            context.highestBatchIdCommitted = highestCommittedBatchID;
            return context;
        }

        // for ACT
        public async Task<TransactionContext> NewTransaction()
        {
            if (numTransactionIdsReserved-- > 0)
            {
                Debug.Assert(tidToAllocate != 0);
                var ctx = new TransactionContext(tidToAllocate++);
                ctx.highestBatchIdCommitted = highestCommittedBatchID;
                return ctx;
            }
            TransactionContext context = null;
            try
            {
                TaskCompletionSource<bool> emitting;
                var myEmitSeq = nonDetEmitSeq;
                if (!nonDetEmitPromiseMap.ContainsKey(myEmitSeq))
                {
                    nonDetEmitPromiseMap.Add(myEmitSeq, new TaskCompletionSource<bool>());
                    nonDeterministicEmitSize.Add(myEmitSeq, 0);
                }
                emitting = nonDetEmitPromiseMap[myEmitSeq];
                nonDeterministicEmitSize[myEmitSeq] = nonDeterministicEmitSize[myEmitSeq] + 1;

                if (emitting.Task.IsCompleted != true) await emitting.Task;
                var tid = nonDetEmitID[myEmitSeq]++;
                context = new TransactionContext(tid);

                nonDeterministicEmitSize[myEmitSeq] = nonDeterministicEmitSize[myEmitSeq] - 1;
                if (nonDeterministicEmitSize[myEmitSeq] == 0)
                {
                    nonDeterministicEmitSize.Remove(myEmitSeq);
                    nonDetEmitID.Remove(myEmitSeq);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
            }
            context.highestBatchIdCommitted = highestCommittedBatchID;
            return context;
        }

        public async Task CheckBackoff(BatchToken token)
        {
            if (detEmitPromiseMap.Count == 0 && nonDetEmitPromiseMap.Count == 0)
            {
                if (token.backoff)
                {
                    Console.WriteLine($"coord {myID} delay");
                    await Task.Delay(TimeSpan.FromMilliseconds(backoffTimeIntervalMSecs / coordList.Count));
                }
                else if (!token.idleToken)
                {
                    token.idleToken = true;
                    token.markedIdleByCoordinator = myID;
                    var curTime = DateTime.Now;
                    token.backOffProbeStartTime = curTime.Hour * 3600 + curTime.Minute * 60 + curTime.Second;
                }
                else if (token.markedIdleByCoordinator == myID)
                {
                    var curTime = DateTime.Now;
                    var curTimeInSecs = curTime.Hour * 3600 + curTime.Minute * 60 + curTime.Second;
                    if (curTimeInSecs - token.backOffProbeStartTime > idleIntervalTillBackOffSecs)
                    {
                        token.backoff = true;   // Token traverses full round being idle, enable backoff
                        Console.WriteLine($"coord {myID} delay");
                        await Task.Delay(TimeSpan.FromMilliseconds(backoffTimeIntervalMSecs / coordList.Count));
                    }
                }
            }
            else
            {
                if (token.backoff) token.backoff = false;
                else if (token.idleToken) token.idleToken = false;
            }
        }

        public async Task PassToken(BatchToken token)
        {
            if (token.highestCommittedBatchID > highestCommittedBatchID) highestCommittedBatchID = token.highestCommittedBatchID;
            numTransactionIdsReserved = 0;    // Reset the range of pre-allocation
            //await CheckBackoff(token);
            var curBatchID = await EmitDeterministicTransactions(token);
            EmitNonDeterministicTransactions(token);
            tidToAllocate = token.lastTransactionID + 1;
            token.lastTransactionID += numTransactionIdsPreAllocated;
            if (token.highestCommittedBatchID < highestCommittedBatchID) token.highestCommittedBatchID = highestCommittedBatchID;
            _ = coordList[neighborID].PassToken(token);
            if (curBatchID > -1) await EmitBatch(curBatchID);
        }

        private void EmitNonDeterministicTransactions(BatchToken token)
        {
            int myEmitSequence = nonDetEmitSeq;
            if (nonDeterministicEmitSize.ContainsKey(myEmitSequence))
            {
                //Estimate a pre-allocation size based on moving average
                var waitingTxns = nonDeterministicEmitSize[myEmitSequence];
                numTransactionIdsPreAllocated = (int)(smoothingPreAllocationFactor * waitingTxns + (1 - smoothingPreAllocationFactor) * numTransactionIdsPreAllocated);
                numTransactionIdsReserved = numTransactionIdsPreAllocated;
                Debug.Assert(nonDetEmitID.ContainsKey(myEmitSequence) == false);
                nonDetEmitID.Add(myEmitSequence, token.lastTransactionID + 1);
                token.lastTransactionID += nonDeterministicEmitSize[myEmitSequence];
                nonDetEmitSeq++;
                nonDetEmitPromiseMap[myEmitSequence].SetResult(true);
                nonDetEmitPromiseMap.Remove(myEmitSequence);
            }
            else
            {
                numTransactionIdsPreAllocated = 0;
                numTransactionIdsReserved = 0;
            }
        }

        private async Task<int> EmitDeterministicTransactions(BatchToken token)
        {
            var myEmitSequence = detEmitSeq;
            if (deterministicTransactionRequests.ContainsKey(myEmitSequence) == false) return -1;
            var transactionList = deterministicTransactionRequests[myEmitSequence];
            Debug.Assert(transactionList.Count > 0);
            detEmitSeq++;
            var curBatchID = token.lastTransactionID + 1;
            foreach (var context in transactionList)
            {
                context.batchID = curBatchID;
                context.transactionID = ++token.lastTransactionID;
                if (batchSchedulePerGrain.ContainsKey(context.batchID) == false)
                    batchSchedulePerGrain.Add(context.batchID, new Dictionary<int, DeterministicBatchSchedule>());
                // update the schedule for each grain accessed by this transaction
                var grainSchedule = batchSchedulePerGrain[context.batchID];
                foreach (var item in context.grainAccessInformation)
                {
                    if (grainSchedule.ContainsKey(item.Key) == false)
                        grainSchedule.Add(item.Key, new DeterministicBatchSchedule(context.batchID));
                    grainSchedule[item.Key].AddNewTransaction(context.transactionID, item.Value);
                }
                context.grainAccessInformation.Clear();

                // if persist PACT input
                if (!pactLogInfo.ContainsKey(curBatchID)) pactLogInfo.Add(curBatchID, new Dictionary<int, Tuple<int, object>>());
                pactLogInfo[curBatchID].Add(context.transactionID, new Tuple<int, object>(context.grainID, context.input));
            }

            var curScheduleMap = batchSchedulePerGrain[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, curScheduleMap.Count);

            // update the last batch ID for each grain accessed by this batch
            foreach (var item in curScheduleMap)
            {
                var grain = item.Key;
                var schedule = item.Value;
                if (token.lastBatchPerGrain.ContainsKey(grain)) schedule.lastBatchID = token.lastBatchPerGrain[grain];
                else schedule.lastBatchID = -1;
                Debug.Assert(schedule.batchID > schedule.lastBatchID);
                token.lastBatchPerGrain[grain] = schedule.batchID;
            }
            lastBatchIDMap.Add(curBatchID, token.lastBatchID);
            if (token.lastBatchID > -1) coordEmitLastBatch.Add(curBatchID, token.lastCoordID);
            token.lastBatchID = curBatchID;
            token.lastCoordID = myID;

            // garbage collection
            if (highestCommittedBatchID > token.highestCommittedBatchID)
            {
                var expiredGrains = new List<int>();
                foreach (var item in token.lastBatchPerGrain)  // only when last batch is already committed, the next emmitted batch can have its lastBid = -1 again
                    if (item.Value <= highestCommittedBatchID) expiredGrains.Add(item.Key);
                foreach (var item in expiredGrains) token.lastBatchPerGrain.Remove(item);
            }

            detEmitPromiseMap[myEmitSequence].SetResult(true);
            deterministicTransactionRequests.Remove(myEmitSequence);
            detEmitPromiseMap.Remove(myEmitSequence);
            batchStartTime.Remove(myEmitSequence);
            //Console.WriteLine($"emit batch {curBatchID}, touches {curScheduleMap.Count} grains, includes {transactionList.Count} txn");
            return curBatchID;
        }

        private async Task EmitBatch(int curBatchID)
        {
            var curScheduleMap = batchSchedulePerGrain[curBatchID];
            /*
            if (log != null)
            {
                var participants = new HashSet<int>();
                participants.UnionWith(curScheduleMap.Keys);
                await log.HandleOnPrepareInDeterministicProtocol(curBatchID, participants);
            }*/

            // if persist PACT input
            try
            {
                if (log != null) await log.HandleOnPrepareInDeterministicProtocol(curBatchID, curScheduleMap, pactLogInfo[curBatchID]);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
            }
            
            foreach (var item in curScheduleMap)
            {
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, grainClassName);
                var schedule = item.Value;
                schedule.globalCoordinator = myID;
                schedule.highestCommittedBatchId = highestCommittedBatchID;
                _ = dest.ReceiveBatchSchedule(schedule);
            }
        }

        /*
         * Grain calls this function to ack its completion of a batch execution
        */
        public async Task AckBatchCompletion(int bid)
        {
            expectedAcksPerBatch[bid]--;
            if (expectedAcksPerBatch[bid] == 0)
            {
                var lastBid = lastBatchIDMap[bid];
                if (highestCommittedBatchID < lastBid)
                {
                    var coord = coordEmitLastBatch[bid];
                    if (coord == myID) await WaitBatchCommit(lastBid);
                    else
                    {
                        var lastCoord = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(coord);
                        await lastCoord.WaitBatchCommit(lastBid);
                    }
                }
                else Debug.Assert(highestCommittedBatchID == lastBid);
                highestCommittedBatchID = bid;
                if (batchesWaitingForCommit.ContainsKey(bid)) batchesWaitingForCommit[bid].SetResult(true);
                _ = NotifyGrains(bid);
                CleanUp(bid);
                if (log != null) await log.HandleOnCommitInDeterministicProtocol(bid);
            }
        }

        public async Task WaitBatchCommit(int bid)
        {
            if (highestCommittedBatchID == bid) return;
            if (batchesWaitingForCommit.ContainsKey(bid) == false) batchesWaitingForCommit.Add(bid, new TaskCompletionSource<bool>());
            await batchesWaitingForCommit[bid].Task;
        }

        private async Task NotifyGrains(int bid)
        {
            foreach (var grain in batchSchedulePerGrain[bid])
            {
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(grain.Key, grainClassName);
                _ = dest.AckBatchCommit(bid);
            }
            await Task.CompletedTask;
        }

        public void CleanUp(int bid)
        {
            expectedAcksPerBatch.Remove(bid);
            batchSchedulePerGrain.Remove(bid);
            coordEmitLastBatch.Remove(bid);
            lastBatchIDMap.Remove(bid);
            if (batchesWaitingForCommit.ContainsKey(bid)) batchesWaitingForCommit.Remove(bid);
        }

        public Task SpawnCoordinator(string grainClassName, int numofCoordinators, int batchInterval, int backoffIntervalMSecs, int idleIntervalTillBackOffSecs, dataFormatType dataFormat, StorageWrapperType logStorage)
        {
            Debug.Assert(deterministicTransactionRequests.Count == 0);

            detEmitSeq = 0;
            nonDetEmitSeq = 0;
            tidToAllocate = -1;
            highestCommittedBatchID = -1;
            numTransactionIdsReserved = 0;
            numTransactionIdsPreAllocated = 0;
            smoothingPreAllocationFactor = 0.5f;
            this.grainClassName = grainClassName;
            backoffTimeIntervalMSecs = backoffIntervalMSecs;
            this.idleIntervalTillBackOffSecs = idleIntervalTillBackOffSecs;

            neighborID = (myID + 1) % numofCoordinators;
            for (int i = 0; i < numofCoordinators; i++)
            {
                var coord = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(i);
                coordList.Add(coord);
            }

            Console.WriteLine($"coord {myID}: batchInterval = {batchInterval}");
            if (idleIntervalTillBackOffSecs > 3600) throw new Exception("Too high value for back off probing -> cannot exceed an 1 hour");

            if (logStorage != StorageWrapperType.NOSTORAGE) log = new Simple2PCLoggingProtocol<string>(GetType().ToString(), myID, dataFormat, logStorage);
            Console.WriteLine($"Coord {myID} initialize logging {logStorage}.");

            return Task.CompletedTask;
        }
    }
}