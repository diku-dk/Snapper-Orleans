using System;
using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.TransactionExecution;
using Orleans.Concurrency;
using System.Linq;
using System.Diagnostics;

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [LocalCoordGrainPlacementStrategy]
    public class LocalCoordGrain : Grain, ILocalCoordGrain
    {
        // coord basic info
        int myID;
        readonly ICoordMap coordMap;
        ILocalCoordGrain neighborCoord;
        Dictionary<int, string> grainClassName;                                             // grainID, grainClassName
        ILoggingProtocol log;
        readonly ILoggerGroup loggerGroup;

        // PACT
        DetTxnProcessor detTxnProcessor;
        Dictionary<long, int> expectedAcksPerBatch;
        Dictionary<long, Dictionary<int, SubBatch>> bidToSubBatches;

        // Hierarchical Architecture
        // for global batches sent from global coordinators
        SortedDictionary<long, SubBatch> globalBatchInfo;                                   // key: global bid
        Dictionary<long, Dictionary<long, List<int>>> globalTransactionInfo;                // <global bid, <global tid, grainAccessInfo>>
        Dictionary<long, TaskCompletionSource<Tuple<long, long>>> globalDetRequestPromise;  // <global tid, <local bid, local tid>>
        Dictionary<long, long> localBidToGlobalBid;
        Dictionary<long, Dictionary<long, long>> globalTidToLocalTidPerBatch;               // local bid, <global tid, local tid>
        // for global batch commitment
        long highestCommittedGlobalBid;
        Dictionary<long, int> globalBidToGlobalCoordID;
        Dictionary<long, bool> globalBidToIsPrevBatchGlobal;                                // global bid, if this batch's previous one is also a global batch
        Dictionary<long, TaskCompletionSource<bool>> globalBatchCommit;                     // global bid, commit promise
        
        // ACT
        NonDetTxnProcessor nonDetTxnProcessor;

        public Task CheckGC()
        {
            detTxnProcessor.CheckGC();
            nonDetTxnProcessor.CheckGC();
            if (expectedAcksPerBatch.Count != 0) Console.WriteLine($"LocalCoord {myID}: expectedAcksPerBatch.Count = {expectedAcksPerBatch.Count}");
            if (bidToSubBatches.Count != 0) Console.WriteLine($"LocalCoord {myID}: bidToSubBatches.Count = {bidToSubBatches.Count}");
            if (globalBatchInfo.Count != 0) Console.WriteLine($"LocalCoord {myID}: globalBatchInfo.Count = {globalBatchInfo.Count}");
            if (globalTransactionInfo.Count != 0) Console.WriteLine($"LocalCoord {myID}: globalTransactionInfo.Count = {globalTransactionInfo.Count}");
            if (globalDetRequestPromise.Count != 0) Console.WriteLine($"LocalCoord {myID}: globalDetRequestPromise.Count = {globalDetRequestPromise.Count}");
            if (localBidToGlobalBid.Count != 0) Console.WriteLine($"LocalCoord {myID}: localBidToGlobalBid.Count = {localBidToGlobalBid.Count}");
            if (globalTidToLocalTidPerBatch.Count != 0) Console.WriteLine($"LocalCoord {myID}: globalTidToLocalTidPerBatch.Count = {globalTidToLocalTidPerBatch.Count}");
            if (globalBidToIsPrevBatchGlobal.Count != 0) Console.WriteLine($"LocalCoord {myID}: globalBidToIsPrevBatchGlobal.Count = {globalBidToIsPrevBatchGlobal.Count}");
            if (globalBatchCommit.Count != 0) Console.WriteLine($"LocalCoord {myID}: globalBatchCommit.Count = {globalBatchCommit.Count}");
            if (globalBidToGlobalCoordID.Count != 0) Console.WriteLine($"LocalCoord {myID}: globalBidToGlobalCoordID.Count = {globalBidToGlobalCoordID.Count}");
            return Task.CompletedTask;
        }

        void Init()
        {
            highestCommittedGlobalBid = -1;
            grainClassName = new Dictionary<int, string>();
            expectedAcksPerBatch = new Dictionary<long, int>();
            bidToSubBatches = new Dictionary<long, Dictionary<int, SubBatch>>();
            globalBatchInfo = new SortedDictionary<long, SubBatch>();
            globalTransactionInfo = new Dictionary<long, Dictionary<long, List<int>>>();
            globalDetRequestPromise = new Dictionary<long, TaskCompletionSource<Tuple<long, long>>>();
            localBidToGlobalBid = new Dictionary<long, long>();
            globalTidToLocalTidPerBatch = new Dictionary<long, Dictionary<long, long>>();
            globalBidToIsPrevBatchGlobal = new Dictionary<long, bool>();
            globalBatchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
            globalBidToGlobalCoordID = new Dictionary<long, int>();
        }

        public override Task OnActivateAsync()
        {
            Init();
            myID = (int)this.GetPrimaryKeyLong();
            nonDetTxnProcessor = new NonDetTxnProcessor(myID);
            detTxnProcessor = new DetTxnProcessor(
                myID,
                coordMap,
                expectedAcksPerBatch,
                bidToSubBatches);
            return base.OnActivateAsync();
        }

        public LocalCoordGrain(ILoggerGroup loggerGroup, ICoordMap coordMap)
        {
            this.loggerGroup = loggerGroup;
            this.coordMap = coordMap;
        }

        public Task ReceiveBatchSchedule(SubBatch batch)
        {
            var globalBid = batch.bid;
            globalBatchInfo.Add(globalBid, batch);
            globalBidToGlobalCoordID.Add(globalBid, batch.coordID);
            if (globalTransactionInfo.ContainsKey(globalBid) == false)
                globalTransactionInfo.Add(globalBid, new Dictionary<long, List<int>>());
            return Task.CompletedTask;
        }

        public async Task<TransactionRegistInfo> NewGlobalTransaction(long globalBid, long globalTid, List<int> grainAccessInfo, List<string> grainClassName)
        {
            for (int i = 0; i < grainAccessInfo.Count; i++)
            {
                var grainID = grainAccessInfo[i];
                if (this.grainClassName.ContainsKey(grainID) == false)
                    this.grainClassName.Add(grainID, grainClassName[i]);
            }
                
            if (globalTransactionInfo.ContainsKey(globalBid) == false)
                globalTransactionInfo.Add(globalBid, new Dictionary<long, List<int>>());
            globalTransactionInfo[globalBid].Add(globalTid, grainAccessInfo);
            
            var promise = new TaskCompletionSource<Tuple<long, long>>();
            globalDetRequestPromise.Add(globalTid, promise);
            await promise.Task;
            return new TransactionRegistInfo(promise.Task.Result.Item1, promise.Task.Result.Item2, detTxnProcessor.highestCommittedBid);
        }

        // for PACT
        public async Task<TransactionRegistInfo> NewTransaction(List<int> grainAccessInfo, List<string> grainClassName)
        {
            var task = detTxnProcessor.NewDet(grainAccessInfo);
            for (int i = 0; i < grainAccessInfo.Count; i++)
            {
                var grain = grainAccessInfo[i];
                if (this.grainClassName.ContainsKey(grain) == false)
                    this.grainClassName.Add(grain, grainClassName[i]);
            }
            var id = await task;
            return new TransactionRegistInfo(id.Item1, id.Item2, detTxnProcessor.highestCommittedBid);
        }

        // for ACT
        public async Task<TransactionRegistInfo> NewTransaction()
        {
            var tid = await nonDetTxnProcessor.NewNonDet();
            return new TransactionRegistInfo(tid, detTxnProcessor.highestCommittedBid);
        }

        public async Task PassToken(LocalToken token)
        {
            long curBatchID;
            var curBatchIDs = new List<long>();
            if (token.isLastEmitBidGlobal)
            {
                ProcessGlobalBatch(token, curBatchIDs);
                curBatchID = detTxnProcessor.GenerateBatch(token);
            }
            else
            {
                curBatchID = detTxnProcessor.GenerateBatch(token);
                ProcessGlobalBatch(token, curBatchIDs);
            }
            
            nonDetTxnProcessor.EmitNonDetTransactions(token);

            if (detTxnProcessor.highestCommittedBid > token.highestCommittedBid)
                detTxnProcessor.GarbageCollectTokenInfo(token);
            else detTxnProcessor.highestCommittedBid = token.highestCommittedBid;

            _ = neighborCoord.PassToken(token);
            if (curBatchID != -1) await EmitBatch(curBatchID);
            if (curBatchIDs.Count != 0)
                foreach (var bid in curBatchIDs) await EmitBatch(bid);
        }

        void ProcessGlobalBatch(LocalToken token, List<long> curBatchIDs)
        {
            while (globalBatchInfo.Count != 0)
            {
                var batch = globalBatchInfo.First();
                var globalBid = batch.Key;
               
                if (batch.Value.lastBid != token.lastEmitGlobalBid) return;
                if (batch.Value.txnList.Count != globalTransactionInfo[globalBid].Count) return;

                var curBatchID = token.lastEmitTid + 1;
                curBatchIDs.Add(curBatchID);
                localBidToGlobalBid.Add(curBatchID, globalBid);
                globalTidToLocalTidPerBatch.Add(curBatchID, new Dictionary<long, long>());

                foreach (var globalTid in batch.Value.txnList)
                {
                    var localTid = ++token.lastEmitTid;
                    globalDetRequestPromise[globalTid].SetResult(new Tuple<long, long>(curBatchID, localTid));

                    var grainAccessInfo = globalTransactionInfo[globalBid][globalTid];
                    detTxnProcessor.GnerateSchedulePerService(localTid, curBatchID, grainAccessInfo);

                    globalTidToLocalTidPerBatch[curBatchID].Add(globalTid, localTid);
                    globalDetRequestPromise.Remove(globalTid);
                }
                globalBidToIsPrevBatchGlobal.Add(globalBid, token.isLastEmitBidGlobal);
                globalBatchInfo.Remove(globalBid);
                globalTransactionInfo.Remove(globalBid);
                detTxnProcessor.UpdateToken(token, curBatchID, globalBid);
                token.lastEmitGlobalBid = globalBid;
            }
        }

        async Task EmitBatch(long bid)
        {
            var curScheduleMap = bidToSubBatches[bid];

            if (log != null) await log.HandleOnPrepareInDeterministicProtocol(bid, new HashSet<int>(curScheduleMap.Keys));

            long globalBid = -1;
            if (localBidToGlobalBid.ContainsKey(bid))
                globalBid = localBidToGlobalBid[bid];

            var globalTidToLocalTid = new Dictionary<long, long>();
            if (globalTidToLocalTidPerBatch.ContainsKey(bid))
            {
                globalTidToLocalTid = globalTidToLocalTidPerBatch[bid];
                globalTidToLocalTidPerBatch.Remove(bid);
            }

            foreach (var item in curScheduleMap)
            {
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, grainClassName[item.Key]);
                var batch = item.Value;

                var localSubBatch = new LocalSubBatch(globalBid, batch);
                localSubBatch.highestCommittedBid = detTxnProcessor.highestCommittedBid;
                localSubBatch.globalTidToLocalTid = globalTidToLocalTid;
                
                _ = dest.ReceiveBatchSchedule(localSubBatch);
            }
        }

        void ACKGlobalCoord(long globalBid)
        {
            var globalCoordID = globalBidToGlobalCoordID[globalBid];
            var globalCoord = coordMap.GetGlobalCoord(globalCoordID);
            _ = globalCoord.AckBatchCompletion(globalBid);
        }

        public async Task AckBatchCompletion(long bid)
        {
            expectedAcksPerBatch[bid]--;
            if (expectedAcksPerBatch[bid] != 0) return;

            // the batch has been completed in this silo
            long globalBid = -1;
            var isPrevGlobal = false;
            var isGlobal = localBidToGlobalBid.ContainsKey(bid);
            
            if (isGlobal)
            {
                // ACK the global coordinator
                globalBid = localBidToGlobalBid[bid];
                isPrevGlobal = globalBidToIsPrevBatchGlobal[globalBid];

                if (isPrevGlobal) ACKGlobalCoord(globalBid);
            }

            await detTxnProcessor.WaitPrevBatchToCommit(bid);

            if (isGlobal)
            {
                if (isPrevGlobal == false) ACKGlobalCoord(globalBid);
                await WaitGlobalBatchCommit(globalBid);

                localBidToGlobalBid.Remove(bid);
                globalBidToGlobalCoordID.Remove(globalBid);
                globalBidToIsPrevBatchGlobal.Remove(globalBid);
            } 

            if (log != null) await log.HandleOnCommitInDeterministicProtocol(bid);

            detTxnProcessor.AckBatchCommit(bid);

            var curScheduleMap = bidToSubBatches[bid];
            foreach (var item in curScheduleMap)
            {
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, grainClassName[item.Key]);
                _ = dest.AckBatchCommit(bid);
            }

            bidToSubBatches.Remove(bid);
            expectedAcksPerBatch.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            await detTxnProcessor.WaitBatchCommit(bid);
        }

        async Task WaitGlobalBatchCommit(long globalBid)
        {
            if (highestCommittedGlobalBid >= globalBid) return;
            if (globalBatchCommit.ContainsKey(globalBid) == false) 
                globalBatchCommit.Add(globalBid, new TaskCompletionSource<bool>());
            await globalBatchCommit[globalBid].Task;
        }

        public Task AckGlobalBatchCommit(long globalBid)
        {
            highestCommittedGlobalBid = Math.Max(globalBid, highestCommittedGlobalBid);
            if (globalBatchCommit.ContainsKey(globalBid))
            {
                globalBatchCommit[globalBid].SetResult(true);
                globalBatchCommit.Remove(globalBid);
            }
            return Task.CompletedTask;
        }

        public Task SpawnLocalCoordGrain()
        {
            highestCommittedGlobalBid = -1;
            detTxnProcessor.Init();
            nonDetTxnProcessor.Init();

            int neighborID;
            if (Constants.multiSilo == false || Constants.hierarchicalCoord)
                neighborID = LocalCoordGrainPlacementHelper.MapCoordIDToNeighborID(myID);
            else neighborID = GlobalCoordGrainPlacementHelper.MapCoordIDToNeighborID(myID);
            
            neighborCoord = GrainFactory.GetGrain<ILocalCoordGrain>(neighborID);

            loggerGroup.GetLoggingProtocol(myID, out log);
            Console.WriteLine($"Local coord {myID} initialize logging {Constants.loggingType}.");

            return Task.CompletedTask;
        }
    }
}