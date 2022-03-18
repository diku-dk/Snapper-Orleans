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

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [GlobalCoordGrainPlacementStrategy]
    public class GlobalCoordGrain : Grain, IGlobalCoordGrain
    {
        // coord basic info
        int myID;
        int highestCommittedBid;
        ILoggerGroup loggerGroup;
        ILoggingProtocol<string> log;
        IGlobalCoordGrain neighborCoord;
        List<IGlobalCoordGrain> coordList;
        DetTxnManager detTxnManager;

        // PACT
        Dictionary<int, int> bidToLastBid;                                 // <bid, lastBid>
        Dictionary<int, int> bidToLastCoordID;                             // <bid, coordID who emit this bid's lastBid>
        Dictionary<int, int> expectedAcksPerBatch;
        Dictionary<int, TaskCompletionSource<bool>> batchCommit;
        Dictionary<int, Dictionary<int, SubBatch>> batchSchedulePerSilo;
        // only for global batches
        Dictionary<int, Dictionary<int, int>> coordPerBatchPerSilo;        // <bid, siloID, localCoordID>

        // ACT
        NonDetTxnManager nonDetTxnManager;

        public override Task OnActivateAsync()
        {
            myID = (int)this.GetPrimaryKeyLong();
            highestCommittedBid = -1;
            coordList = new List<IGlobalCoordGrain>();
            bidToLastBid = new Dictionary<int, int>();
            bidToLastCoordID = new Dictionary<int, int>();
            expectedAcksPerBatch = new Dictionary<int, int>();
            batchCommit = new Dictionary<int, TaskCompletionSource<bool>>();
            batchSchedulePerSilo = new Dictionary<int, Dictionary<int, SubBatch>>();
            coordPerBatchPerSilo = new Dictionary<int, Dictionary<int, int>>();
            nonDetTxnManager = new NonDetTxnManager(myID);
            detTxnManager = new DetTxnManager(
                myID,
                bidToLastBid,
                bidToLastCoordID,
                expectedAcksPerBatch,
                batchSchedulePerSilo,
                coordPerBatchPerSilo);
            return base.OnActivateAsync();
        }

        public GlobalCoordGrain(ILoggerGroup loggerGroup)
        {
            this.loggerGroup = loggerGroup;
        }

        public Task CheckGC()
        {
            if (bidToLastBid.Count != 0) Console.WriteLine($"GlobalCoord {myID}: bidToLastBid.Count = {bidToLastBid.Count}");
            if (bidToLastCoordID.Count != 0) Console.WriteLine($"GlobalCoord {myID}: bidToLastCoordID.Count = {bidToLastCoordID.Count}");
            if (expectedAcksPerBatch.Count != 0) Console.WriteLine($"GlobalCoord {myID}: expectedAcksPerBatch.Count = {expectedAcksPerBatch.Count}");
            if (batchCommit.Count != 0) Console.WriteLine($"GlobalCoord {myID}: batchCommit.Count = {batchCommit.Count}");
            if (batchSchedulePerSilo.Count != 0) Console.WriteLine($"GlobalCoord {myID}: batchSchedulePerSilo.Count = {batchSchedulePerSilo.Count}");
            if (coordPerBatchPerSilo.Count != 0) Console.WriteLine($"GlobalCoord {myID}: coordPerBatchPerSilo.Count = {coordPerBatchPerSilo.Count}");
            nonDetTxnManager.CheckGC();
            detTxnManager.CheckGC();
            return Task.CompletedTask;
        }

        // for PACT
        public async Task<TransactionRegistInfo> NewTransaction(List<int> siloList, List<int> coordList)
        {
            var id = await detTxnManager.NewDet(siloList, coordList);
            return new TransactionRegistInfo(id.Item1, id.Item2, highestCommittedBid);
        }

        // for ACT
        public async Task<TransactionRegistInfo> NewTransaction()
        {
            var tid = await nonDetTxnManager.NewNonDet();
            return new TransactionRegistInfo(tid, highestCommittedBid);
        }

        public Task PassToken(BasicToken token)
        {
            var curBatchID = detTxnManager.GenerateBatch(token);
            nonDetTxnManager.EmitNonDetTransactions(token);
            detTxnManager.GarbageCollectTokenInfo(highestCommittedBid, token);
            highestCommittedBid = Math.Max(highestCommittedBid, token.highestCommittedBid);
            _ = neighborCoord.PassToken(token);
            if (curBatchID != -1) _ = EmitBatch(curBatchID);
            return Task.CompletedTask;
        }

        async Task EmitBatch(int curBatchID)
        {
            var curScheduleMap = batchSchedulePerSilo[curBatchID];
            if (log != null) await log.HandleOnPrepareInDeterministicProtocol(curBatchID, new HashSet<int>(curScheduleMap.Keys));

            var coords = coordPerBatchPerSilo[curBatchID];
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];
                var dest = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID);
                _ = dest.ReceiveBatchSchedule(item.Value);
            }
        }
        /*
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
                        var lastCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(coord);
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
            foreach (var item in batchSchedulePerSilo[bid])
            {
                var dest = GrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, grainClassName[item.Key]);
                _ = dest.AckBatchCommit(bid);
            }
            await Task.CompletedTask;
        }

        void CleanUp(int bid)
        {
            expectedAcksPerBatch.Remove(bid);
            batchSchedulePerSilo.Remove(bid);
            coordListPerTxn.Remove(bid);
            bidToLastCoordID.Remove(bid);
            bidToLastBid.Remove(bid);
            if (batchCommit.ContainsKey(bid)) batchCommit.Remove(bid);
        }
        */
        public Task SpawnGlobalCoordGrain()
        {
            highestCommittedBid = -1;
            nonDetTxnManager.Init();

            var neighborID = GlobalCoordGrainPlacementHelper.MapCoordIDToNeighborID(myID);
            neighborCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(neighborID);

            for (int i = 0; i < Constants.numGlobalCoord; i++)
            {
                var coord = GrainFactory.GetGrain<IGlobalCoordGrain>(i);
                coordList.Add(coord);     // add all global coordinators to the list
            }

            var numLogger = Constants.numGlobalLogger;
          
            if (Constants.loggingType == LoggingType.LOGGER)
            {
                var loggerID = Helper.MapGrainIDToServiceID(myID, numLogger);
                var logger = loggerGroup.GetSingleton(loggerID);
                log = new Simple2PCLoggingProtocol<string>(GetType().ToString(), myID, logger);
            }
            else if (Constants.loggingType == LoggingType.ONGRAIN)
                log = new Simple2PCLoggingProtocol<string>(GetType().ToString(), myID);

            Console.WriteLine($"Global coord {myID} initialize logging {Constants.loggingType}.");

            return Task.CompletedTask;
        }
    }
}