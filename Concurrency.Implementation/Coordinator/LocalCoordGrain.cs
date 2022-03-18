using System;
using Orleans;
using Utilities;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.TransactionExecution;
using Orleans.Concurrency;
using System.Threading;

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [LocalCoordGrainPlacementStrategy]
    public class LocalCoordGrain : Grain, ILocalCoordGrain
    {
        // coord basic info
        int myID;
        int highestCommittedBid;
        ILoggerGroup loggerGroup;
        ILoggingProtocol<string> log;
        ILocalCoordGrain neighborCoord;
        List<ILocalCoordGrain> coordList;
        Dictionary<int, string> grainClassName;                                          // grainID, grainClassName
        DetTxnManager detTxnManager;

        // PACT
        Dictionary<int, int> bidToLastBid;                                               // <bid, lastBid>
        Dictionary<int, int> bidToLastCoordID;                                           // <bid, coordID who emit this bid's lastBid>
        Dictionary<int, int> expectedAcksPerBatch; 
        Dictionary<int, TaskCompletionSource<bool>> batchCommit;
        Dictionary<int, Dictionary<int, SubBatch>> batchSchedulePerGrain;

        // for global batches sent from global coordinators
        SortedDictionary<int, SubBatch> globalBatchInfo;                                 // key: global bid
        Dictionary<int, CountdownEvent> globalBatchPromise;                              // key: global bid, make sure we have received all transactions' grainAccessInfo
        Dictionary<int, List<int>> globalTransactionInfo;                                // <global tid, grainAccessInfo>
        Dictionary<int, TaskCompletionSource<Tuple<int, int>>> globalDetRequestPromise;  // <global tid, <local bid, local tid>>
        Dictionary<int, int> localBidToGlobalBid;
        Dictionary<int, Dictionary<int, int>> globalTidToLocalTidPerBatch;               // local bid, <global tid, local tid>

        // ACT
        NonDetTxnManager nonDetTxnManager;

        public override Task OnActivateAsync()
        {
            myID = (int)this.GetPrimaryKeyLong();
            highestCommittedBid = -1;
            coordList = new List<ILocalCoordGrain>();
            grainClassName = new Dictionary<int, string>();
            bidToLastBid = new Dictionary<int, int>();
            bidToLastCoordID = new Dictionary<int, int>();
            expectedAcksPerBatch = new Dictionary<int, int>();
            batchCommit = new Dictionary<int, TaskCompletionSource<bool>>();
            batchSchedulePerGrain = new Dictionary<int, Dictionary<int, SubBatch>>();
            globalBatchInfo = new SortedDictionary<int, SubBatch>();
            globalBatchPromise = new Dictionary<int, CountdownEvent>();
            globalTransactionInfo = new Dictionary<int, List<int>>();
            globalDetRequestPromise = new Dictionary<int, TaskCompletionSource<Tuple<int, int>>>();
            localBidToGlobalBid = new Dictionary<int, int>();
            globalTidToLocalTidPerBatch = new Dictionary<int, Dictionary<int, int>>();
            nonDetTxnManager = new NonDetTxnManager(myID);
            detTxnManager = new DetTxnManager(
                myID,
                bidToLastBid,
                bidToLastCoordID,
                expectedAcksPerBatch,
                batchSchedulePerGrain);
            return base.OnActivateAsync();
        }

        public LocalCoordGrain(ILoggerGroup loggerGroup)
        {
            this.loggerGroup = loggerGroup;
        }

        public Task CheckGC()
        {
            if (bidToLastBid.Count != 0) Console.WriteLine($"GlobalCoord {myID}: bidToLastBid.Count = {bidToLastBid.Count}");
            if (bidToLastCoordID.Count != 0) Console.WriteLine($"GlobalCoord {myID}: bidToLastCoordID.Count = {bidToLastCoordID.Count}");
            if (expectedAcksPerBatch.Count != 0) Console.WriteLine($"GlobalCoord {myID}: expectedAcksPerBatch.Count = {expectedAcksPerBatch.Count}");
            if (batchCommit.Count != 0) Console.WriteLine($"GlobalCoord {myID}: batchCommit.Count = {batchCommit.Count}");
            if (batchSchedulePerGrain.Count != 0) Console.WriteLine($"GlobalCoord {myID}: batchSchedulePerGrain.Count = {batchSchedulePerGrain.Count}");
            if (globalBatchInfo.Count != 0) Console.WriteLine($"GlobalCoord {myID}: globalBatchInfo.Count = {globalBatchInfo.Count}");
            if (globalBatchPromise.Count != 0) Console.WriteLine($"GlobalCoord {myID}: globalBatchPromise.Count = {globalBatchPromise.Count}");
            if (globalTransactionInfo.Count != 0) Console.WriteLine($"GlobalCoord {myID}: globalTransactionInfo.Count = {globalTransactionInfo.Count}");
            if (globalDetRequestPromise.Count != 0) Console.WriteLine($"GlobalCoord {myID}: globalDetRequestPromise.Count = {globalDetRequestPromise.Count}");
            nonDetTxnManager.CheckGC();
            detTxnManager.CheckGC();
            return Task.CompletedTask;
        }

        public Task ReceiveBatchSchedule(SubBatch batch)
        {
            globalBatchInfo.Add(batch.bid, batch);
            var countDown = new CountdownEvent(batch.txnList.Count);
            globalBatchPromise.Add(batch.bid, countDown);
            return Task.CompletedTask;
        }

        public async Task<TransactionRegistInfo> NewGlobalTransaction(int globalTid, List<int> grainAccessInfo)
        {
            globalTransactionInfo.Add(globalTid, grainAccessInfo);
            var promise = new TaskCompletionSource<Tuple<int, int>>();
            globalDetRequestPromise.Add(globalTid, promise);
            await promise.Task;
            return new TransactionRegistInfo(promise.Task.Result.Item1, promise.Task.Result.Item2, highestCommittedBid);
        }

        // for PACT
        public async Task<TransactionRegistInfo> NewTransaction(List<int> grainAccessInfo, List<string> grainClassName)
        {
            var task = detTxnManager.NewDet(grainAccessInfo);
            for (int i = 0; i < grainAccessInfo.Count; i++)
            {
                var grain = grainAccessInfo[i];
                if (this.grainClassName.ContainsKey(grain) == false)
                    this.grainClassName.Add(grain, grainClassName[i]);
            }
            var id = await task;
            return new TransactionRegistInfo(id.Item1, id.Item2, highestCommittedBid);
        }

        // for ACT
        public async Task<TransactionRegistInfo> NewTransaction()
        {
            var tid = await nonDetTxnManager.NewNonDet();
            return new TransactionRegistInfo(tid, highestCommittedBid);
        }

        public Task PassToken(LocalToken token)
        {
            var curBatchID = detTxnManager.GenerateBatch(token.basicToken);
            var curBatchIDs = ProcessGlobalBatch(token);
            nonDetTxnManager.EmitNonDetTransactions(token.basicToken);
            detTxnManager.GarbageCollectTokenInfo(highestCommittedBid , token.basicToken);
            highestCommittedBid = Math.Max(highestCommittedBid, token.highestCommittedBid);
            _ = neighborCoord.PassToken(token);
            if (curBatchID != -1) _ = EmitBatch(curBatchID);
            if (curBatchIDs.Count != 0)
                foreach (var bid in curBatchIDs) _ = EmitBatch(bid);
            return Task.CompletedTask;
        }

        List<int> ProcessGlobalBatch(LocalToken token)
        {
            var curBatchIDs = new List<int>();
            foreach (var batch in globalBatchInfo)
            {
                var globalBid = batch.Key;
                if (batch.Value.lastBid != token.lastEmitGlobalBid) return curBatchIDs;
                if (globalBatchPromise[globalBid].CurrentCount != 0) return curBatchIDs;
                globalBatchInfo[globalBid].txnList.Clear();
                globalBatchPromise.Remove(globalBid);

                var curBatchID = token.basicToken.lastEmitTid + 1;
                curBatchIDs.Add(curBatchID);
                localBidToGlobalBid.Add(curBatchID, globalBid);
                globalTidToLocalTidPerBatch.Add(curBatchID, new Dictionary<int, int>());
                foreach (var globalTid in batch.Value.txnList)
                {
                    var localTid = ++token.basicToken.lastEmitTid;
                    globalDetRequestPromise[globalTid].SetResult(new Tuple<int, int>(curBatchID, localTid));

                    var grainAccessInfo = globalTransactionInfo[globalTid];
                    detTxnManager.GnerateSchedulePerService(localTid, curBatchID, grainAccessInfo);

                    globalTidToLocalTidPerBatch[curBatchID].Add(globalTid, localTid);

                    globalDetRequestPromise.Remove(globalTid);
                    globalTransactionInfo.Remove(globalTid);
                }
                detTxnManager.UpdateToken(token.basicToken, curBatchID);
                token.lastEmitGlobalBid = globalBid;
            }

            return curBatchIDs;
        }

        async Task EmitBatch(int curBatchID)
        {
            var curScheduleMap = batchSchedulePerGrain[curBatchID];

            if (log != null) await log.HandleOnPrepareInDeterministicProtocol(curBatchID, new HashSet<int>(curScheduleMap.Keys));
            
            foreach (var item in curScheduleMap)
            {
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, grainClassName[item.Key]);
                var batch = item.Value;

                var globalBid = -1;
                if (localBidToGlobalBid.ContainsKey(curBatchID)) globalBid = localBidToGlobalBid[curBatchID];
                var localSubBatch = new LocalSubBatch(globalBid, curBatchID, myID);
                localSubBatch.globalTidToLocalTid = globalTidToLocalTidPerBatch[curBatchID];

                 _ = dest.ReceiveBatchSchedule(localSubBatch);
            }
        }

        public async Task AckBatchCompletion(int bid)
        {
            expectedAcksPerBatch[bid]--;
            if (expectedAcksPerBatch[bid] == 0)
            {
                var lastBid = bidToLastBid[bid];
                if (highestCommittedBid < lastBid)
                {
                    var coord = bidToLastCoordID[bid];
                    if (coord == myID) await WaitBatchCommit(lastBid);
                    else
                    {
                        var lastCoord = GrainFactory.GetGrain<ILocalCoordGrain>(coord);
                        await lastCoord.WaitBatchCommit(lastBid);
                    }
                }
                else Debug.Assert(highestCommittedBid == lastBid);
                highestCommittedBid = bid;
                if (batchCommit.ContainsKey(bid)) batchCommit[bid].SetResult(true);
                _ = NotifyGrains(bid);
                CleanUp(bid);
                if (log != null) await log.HandleOnCommitInDeterministicProtocol(bid);
            }
        }

        public async Task WaitBatchCommit(int bid)
        {
            if (highestCommittedBid == bid) return;
            if (batchCommit.ContainsKey(bid) == false) batchCommit.Add(bid, new TaskCompletionSource<bool>());
            await batchCommit[bid].Task;
        }

        async Task NotifyGrains(int bid)
        {
            foreach (var item in batchSchedulePerGrain[bid])
            {
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, grainClassName[item.Key]);
                _ = dest.AckBatchCommit(bid);
            }
            await Task.CompletedTask;
        }

        void CleanUp(int bid)
        {
            expectedAcksPerBatch.Remove(bid);
            batchSchedulePerGrain.Remove(bid);
            bidToLastCoordID.Remove(bid);
            bidToLastBid.Remove(bid);
            if (batchCommit.ContainsKey(bid)) batchCommit.Remove(bid);
        }

        public Task SpawnLocalCoordGrain()
        {
            highestCommittedBid = -1;
            nonDetTxnManager.Init();

            int numLogger;
            if (Constants.multiSilo == false || Constants.hierarchicalCoord)
            {
                SetHierarchicalArchitecture();
                numLogger = Constants.numLoggerPerSilo;
            }
            else
            {
                SetSimpleArchitecture();
                numLogger = Constants.numGlobalLogger;
            }

            if (Constants.loggingType == LoggingType.LOGGER)
            {
                var loggerID = Helper.MapGrainIDToServiceID(myID, numLogger);
                var logger = loggerGroup.GetSingleton(loggerID);
                log = new Simple2PCLoggingProtocol<string>(GetType().ToString(), myID, logger);
            }
            else if (Constants.loggingType == LoggingType.ONGRAIN)
                log = new Simple2PCLoggingProtocol<string>(GetType().ToString(), myID);

            Console.WriteLine($"Local coord {myID} initialize logging {Constants.loggingType}.");

            return Task.CompletedTask;
        }

        void SetHierarchicalArchitecture()
        {
            var neighborID = LocalCoordGrainPlacementHelper.MapCoordIDToNeighborID(myID);
            neighborCoord = GrainFactory.GetGrain<ILocalCoordGrain>(neighborID);

            var siloID = LocalCoordGrainPlacementHelper.MapCoordIDToSiloID(myID);
            var firstCoordID = LocalCoordGrainPlacementHelper.MapSiloIDToFirstLocalCoordID(siloID);
            for (int i = 0; i < Constants.numLocalCoordPerSilo; i++)
            {
                var coordID = firstCoordID + i;
                var coord = GrainFactory.GetGrain<ILocalCoordGrain>(coordID);
                coordList.Add(coord);     // add all local coordinators in this silo to the list
            }
        }

        void SetSimpleArchitecture()
        {
            var neighborID = GlobalCoordGrainPlacementHelper.MapCoordIDToNeighborID(myID);
            neighborCoord = GrainFactory.GetGrain<ILocalCoordGrain>(neighborID);

            for (int i = 0; i < Constants.numGlobalCoord; i++)
            {
                var coord = GrainFactory.GetGrain<ILocalCoordGrain>(i);
                coordList.Add(coord);     // add all local coordinators to the list
            }
        }
    }
}