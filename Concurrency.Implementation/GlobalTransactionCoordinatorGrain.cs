using System;
using Orleans;
using Utilities;
using Persist.Interfaces;
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
        private int highestCommittedBid;
        private ILoggingProtocol<string> log;
        private int detEmitSeq, nonDetEmitSeq;
        private float smoothingPreAllocationFactor;
        private Dictionary<int, int> lastBatchIDMap;       // <bid, lastBid>
        private Dictionary<int, int> coordEmitLastBatch;   // <bid, coordID who emit lastBid>
        private Dictionary<int, int> expectedAcksPerBatch;
        private IPersistSingletonGroup persistSingletonGroup;
        private List<IGlobalTransactionCoordinatorGrain> coordList;
        private Dictionary<int, TaskCompletionSource<bool>> batchesWaitingForCommit;
        Dictionary<int, TaskCompletionSource<bool>> detEmitPromiseMap, nonDetEmitPromiseMap;

        private Dictionary<int, Dictionary<int, DeterministicBatchSchedule>> batchSchedulePerGrain;  // <bid, GrainID, batch schedule>
        private Dictionary<int, string> grainClassName;   // GrainID, namespace

        Dictionary<int, int> nonDetEmitID;
        Dictionary<int, int> nonDeterministicEmitSize;

        // List buffering the incoming deterministic transaction requests
        Dictionary<int, List<TransactionContext>> deterministicTransactionRequests;
        private int numtidsPreAllocated;
        private int numtidsReserved;
        private int tidToAllocate;

        public override Task OnActivateAsync()
        {
            detEmitSeq = 0;
            nonDetEmitSeq = 0;
            tidToAllocate = -1;
            highestCommittedBid = -1;
            numtidsReserved = 0;
            numtidsPreAllocated = 0;
            smoothingPreAllocationFactor = 0.5f;
            myID = (int)this.GetPrimaryKeyLong();
            nonDetEmitID = new Dictionary<int, int>();
            lastBatchIDMap = new Dictionary<int, int>();
            grainClassName = new Dictionary<int, string>();
            coordEmitLastBatch = new Dictionary<int, int>();
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

        public GlobalTransactionCoordinatoGrain(IPersistSingletonGroup persistSingletonGroup)
        {
            this.persistSingletonGroup = persistSingletonGroup;
        }

        // for PACT
        public async Task<TransactionContext> NewTransaction(Dictionary<int, Tuple<string, int>> grainAccessInformation)
        {
            var myEmitSeq = detEmitSeq;
            var context = new TransactionContext(grainAccessInformation);
            if (deterministicTransactionRequests.ContainsKey(myEmitSeq) == false)
            {
                deterministicTransactionRequests.Add(myEmitSeq, new List<TransactionContext>());
                detEmitPromiseMap.Add(myEmitSeq, new TaskCompletionSource<bool>());
            }
            deterministicTransactionRequests[myEmitSeq].Add(context);
            var emitting = detEmitPromiseMap[myEmitSeq].Task;
            if (emitting.IsCompleted != true) await emitting;
            context.highestCommittedBid = highestCommittedBid;
            return context;
        }

        // for ACT
        public async Task<TransactionContext> NewTransaction()
        {
            if (numtidsReserved-- > 0)
            {
                Debug.Assert(tidToAllocate != 0);
                var ctx = new TransactionContext(tidToAllocate++);
                ctx.highestCommittedBid = highestCommittedBid;
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
            context.highestCommittedBid = highestCommittedBid;
            return context;
        }

        public async Task PassToken(BatchToken token)
        {
            if (token.highestCommittedBid > highestCommittedBid) highestCommittedBid = token.highestCommittedBid;
            numtidsReserved = 0;    // Reset the range of pre-allocation
            var curBatchID = await EmitDeterministicTransactions(token);
            EmitNonDeterministicTransactions(token);
            tidToAllocate = token.lastTid + 1;
            token.lastTid += numtidsPreAllocated;
            if (token.highestCommittedBid < highestCommittedBid) token.highestCommittedBid = highestCommittedBid;
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
                numtidsPreAllocated = (int)(smoothingPreAllocationFactor * waitingTxns + (1 - smoothingPreAllocationFactor) * numtidsPreAllocated);
                numtidsReserved = numtidsPreAllocated;
                Debug.Assert(nonDetEmitID.ContainsKey(myEmitSequence) == false);
                nonDetEmitID.Add(myEmitSequence, token.lastTid + 1);
                token.lastTid += nonDeterministicEmitSize[myEmitSequence];
                nonDetEmitSeq++;
                nonDetEmitPromiseMap[myEmitSequence].SetResult(true);
                nonDetEmitPromiseMap.Remove(myEmitSequence);
            }
            else
            {
                numtidsPreAllocated = 0;
                numtidsReserved = 0;
            }
        }

        private async Task<int> EmitDeterministicTransactions(BatchToken token)
        {
            var myEmitSequence = detEmitSeq;
            if (deterministicTransactionRequests.ContainsKey(myEmitSequence) == false) return -1;
            var transactionList = deterministicTransactionRequests[myEmitSequence];
            Debug.Assert(transactionList.Count > 0);
            detEmitSeq++;
            var curBatchID = token.lastTid + 1;
            foreach (var context in transactionList)
            {
                context.bid = curBatchID;
                context.tid = ++token.lastTid;
                if (batchSchedulePerGrain.ContainsKey(context.bid) == false)
                    batchSchedulePerGrain.Add(context.bid, new Dictionary<int, DeterministicBatchSchedule>());
                // update the schedule for each grain accessed by this transaction
                var grainSchedule = batchSchedulePerGrain[context.bid];
                foreach (var item in context.grainAccessInfo)
                {
                    if (grainClassName.ContainsKey(item.Key) == false)
                        grainClassName.Add(item.Key, item.Value.Item1);
                    if (grainSchedule.ContainsKey(item.Key) == false)
                        grainSchedule.Add(item.Key, new DeterministicBatchSchedule(context.bid));
                    grainSchedule[item.Key].AddNewTransaction(context.tid, item.Value.Item2);
                }
                context.grainAccessInfo.Clear();
            }

            var curScheduleMap = batchSchedulePerGrain[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, curScheduleMap.Count);

            // update the last batch ID for each grain accessed by this batch
            foreach (var grain in curScheduleMap)
            {
                var schedule = grain.Value;
                if (token.lastBidPerGrain.ContainsKey(grain.Key)) schedule.lastBid = token.lastBidPerGrain[grain.Key].Item2;
                else schedule.lastBid = -1;
                Debug.Assert(schedule.bid > schedule.lastBid);
                token.lastBidPerGrain[grain.Key] = new Tuple<string, int>(grainClassName[grain.Key], schedule.bid);
            }
            lastBatchIDMap.Add(curBatchID, token.lastBid);
            if (token.lastBid > -1) coordEmitLastBatch.Add(curBatchID, token.lastCoordID);
            token.lastBid = curBatchID;
            token.lastCoordID = myID;

            // garbage collection
            if (highestCommittedBid > token.highestCommittedBid)
            {
                var expiredGrains = new HashSet<int>();
                foreach (var item in token.lastBidPerGrain)  // only when last batch is already committed, the next emmitted batch can have its lastBid = -1 again
                    if (item.Value.Item2 <= highestCommittedBid) expiredGrains.Add(item.Key);
                foreach (var item in expiredGrains) token.lastBidPerGrain.Remove(item);
            }
            detEmitPromiseMap[myEmitSequence].SetResult(true);
            deterministicTransactionRequests.Remove(myEmitSequence);
            detEmitPromiseMap.Remove(myEmitSequence);
            return curBatchID;
        }

        private async Task EmitBatch(int curBatchID)
        {
            var curScheduleMap = batchSchedulePerGrain[curBatchID];

            if (log != null)
            {
                var participants = new HashSet<int>();  // <grain namespace, grainIDs>
                participants.UnionWith(curScheduleMap.Keys);
                await log.HandleOnPrepareInDeterministicProtocol(curBatchID, participants);
            }
            foreach (var item in curScheduleMap)
            {
                //Console.WriteLine($"batch {curBatchID}: {grainClassName[item.Key]} {item.Key}");
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, grainClassName[item.Key]);
                var schedule = item.Value;
                schedule.coordID = myID;
                schedule.highestCommittedBid = highestCommittedBid;
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
                if (highestCommittedBid < lastBid)
                {
                    var coord = coordEmitLastBatch[bid];
                    if (coord == myID) await WaitBatchCommit(lastBid);
                    else
                    {
                        var lastCoord = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(coord);
                        await lastCoord.WaitBatchCommit(lastBid);
                    }
                }
                else Debug.Assert(highestCommittedBid == lastBid);
                highestCommittedBid = bid;
                if (batchesWaitingForCommit.ContainsKey(bid)) batchesWaitingForCommit[bid].SetResult(true);
                _ = NotifyGrains(bid);
                CleanUp(bid);
                if (log != null) await log.HandleOnCommitInDeterministicProtocol(bid);
            }
        }

        public async Task WaitBatchCommit(int bid)
        {
            if (highestCommittedBid == bid) return;
            if (batchesWaitingForCommit.ContainsKey(bid) == false) batchesWaitingForCommit.Add(bid, new TaskCompletionSource<bool>());
            await batchesWaitingForCommit[bid].Task;
        }

        private async Task NotifyGrains(int bid)
        {
            foreach (var item in batchSchedulePerGrain[bid])
            {
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, grainClassName[item.Key]);
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

        public Task SpawnCoordinator()
        {
            Debug.Assert(deterministicTransactionRequests.Count == 0);

            detEmitSeq = 0;
            nonDetEmitSeq = 0;
            tidToAllocate = -1;
            highestCommittedBid = -1;
            numtidsReserved = 0;
            numtidsPreAllocated = 0;
            smoothingPreAllocationFactor = 0.5f;

            neighborID = (myID + 1) % Constants.numCoordPerSilo;
            for (int i = 0; i < Constants.numCoordPerSilo; i++)
            {
                var coord = GrainFactory.GetGrain<IGlobalTransactionCoordinatorGrain>(i);
                coordList.Add(coord);
            }

            switch (Constants.loggingType)
            {
                case LoggingType.NOLOGGING:
                    break;
                case LoggingType.ONGRAIN:
                    log = new Simple2PCLoggingProtocol<string>(GetType().ToString(), myID);
                    break;
                case LoggingType.PERSISTGRAIN:
                    var persistGrainID = Helper.MapGrainIDToPersistItemID(Constants.numPersistItemPerSilo, myID);
                    var persistGrain = GrainFactory.GetGrain<IPersistGrain>(persistGrainID);
                    log = new Simple2PCLoggingProtocol<string>(GetType().ToString(), myID, persistGrain);
                    break;
                case LoggingType.PERSISTSINGLETON:
                    var persistWorkerID = Helper.MapGrainIDToPersistItemID(Constants.numPersistItemPerSilo, myID);
                    var persistWorker = persistSingletonGroup.GetSingleton(persistWorkerID);
                    log = new Simple2PCLoggingProtocol<string>(GetType().ToString(), myID, persistWorker);
                    break;
                default:
                    throw new Exception($"Exception: Unknown loggingType {Constants.loggingType}");
            }
            Console.WriteLine($"Coord {myID} initialize logging {Constants.loggingType}.");

            return Task.CompletedTask;
        }
    }
}