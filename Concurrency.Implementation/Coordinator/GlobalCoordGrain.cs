using System;
using Orleans;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.GrainPlacement;
using Orleans.Concurrency;
using System.Diagnostics;

namespace Concurrency.Implementation.Coordinator
{
    [Reentrant]
    [GlobalCoordGrainPlacementStrategy]
    public class GlobalCoordGrain : Grain, IGlobalCoordGrain
    {
        // coord basic info
        int myID;
        ICoordMap coordMap;
        ILoggerGroup loggerGroup;
        ILoggingProtocol log;
        IGlobalCoordGrain neighborCoord;

        // PACT
        DetTxnProcessor detTxnProcessor;
        Dictionary<long, int> expectedAcksPerBatch;
        Dictionary<long, Dictionary<int, SubBatch>> bidToSubBatches;
        // only for global batches (Hierarchical Architecture)
        Dictionary<long, Dictionary<int, int>> coordPerBatchPerSilo;        // <global bid, siloID, chosen local Coord ID>

        // ACT
        NonDetTxnProcessor nonDetTxnProcessor;

        DateTime timeOfBatchGeneration;
        double batchSizeInMSecs;

        public Task CheckGC()
        {
            detTxnProcessor.CheckGC();
            nonDetTxnProcessor.CheckGC();
            if (expectedAcksPerBatch.Count != 0) Console.WriteLine($"GlobalCoord {myID}: expectedAcksPerBatch.Count = {expectedAcksPerBatch.Count}");
            if (bidToSubBatches.Count != 0) Console.WriteLine($"GlobalCoord {myID}: batchSchedulePerSilo.Count = {bidToSubBatches.Count}");
            if (coordPerBatchPerSilo.Count != 0) Console.WriteLine($"GlobalCoord {myID}: coordPerBatchPerSilo.Count = {coordPerBatchPerSilo.Count}");
            return Task.CompletedTask;
        }

        public override Task OnActivateAsync()
        {
            myID = (int)this.GetPrimaryKeyLong();
            expectedAcksPerBatch = new Dictionary<long, int>();
            bidToSubBatches = new Dictionary<long, Dictionary<int, SubBatch>>();
            coordPerBatchPerSilo = new Dictionary<long, Dictionary<int, int>>();
            nonDetTxnProcessor = new NonDetTxnProcessor(myID);
            detTxnProcessor = new DetTxnProcessor(
                myID,
                coordMap,
                expectedAcksPerBatch,
                bidToSubBatches,
                coordPerBatchPerSilo);
            return base.OnActivateAsync();
        }

        public GlobalCoordGrain(ILoggerGroup loggerGroup, ICoordMap coordMap)
        {
            this.loggerGroup = loggerGroup;
            this.coordMap = coordMap;
        }

        // for PACT
        public async Task<Tuple<TransactionRegistInfo, Dictionary<int, int>>> NewTransaction(List<int> siloList)
        {
            var id = await detTxnProcessor.NewDet(siloList);
            Debug.Assert(coordPerBatchPerSilo.ContainsKey(id.Item1));
            var info = new TransactionRegistInfo(id.Item1, id.Item2, detTxnProcessor.highestCommittedBid);  // bid, tid, highest committed bid
            return new Tuple<TransactionRegistInfo, Dictionary<int, int>>(info, coordPerBatchPerSilo[id.Item1]);
        }

        // for ACT
        public async Task<TransactionRegistInfo> NewTransaction()
        {
            var tid = await nonDetTxnProcessor.NewNonDet();
            return new TransactionRegistInfo(tid, detTxnProcessor.highestCommittedBid);
        }

        public Task PassToken(BasicToken token)
        {
            long curBatchID = -1;
            var elapsedTime = (DateTime.Now - timeOfBatchGeneration).TotalMilliseconds;
            if (elapsedTime >= batchSizeInMSecs)
            {
                curBatchID = detTxnProcessor.GenerateBatch(token);
                if (curBatchID != -1) timeOfBatchGeneration = DateTime.Now;
            } 
            
            nonDetTxnProcessor.EmitNonDetTransactions(token);

            if (detTxnProcessor.highestCommittedBid > token.highestCommittedBid)
                token.highestCommittedBid = detTxnProcessor.highestCommittedBid;
            else detTxnProcessor.highestCommittedBid = token.highestCommittedBid;

            _ = neighborCoord.PassToken(token);
            if (curBatchID != -1) _ = EmitBatch(curBatchID);
            return Task.CompletedTask;
        }

        async Task EmitBatch(long bid)
        {
            var curScheduleMap = bidToSubBatches[bid];
            if (log != null) await log.HandleOnPrepareInDeterministicProtocol(bid, new HashSet<int>(curScheduleMap.Keys));

            var coords = coordPerBatchPerSilo[bid];
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];
                var dest = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID);
                _ = dest.ReceiveBatchSchedule(item.Value);
            }
        }

        public async Task AckBatchCompletion(long bid)
        {
            // count down the number of expected ACKs from different silos
            expectedAcksPerBatch[bid]--;
            if (expectedAcksPerBatch[bid] != 0) return;

            // commit the batch
            await detTxnProcessor.WaitPrevBatchToCommit(bid);
            if (log != null) await log.HandleOnCommitInDeterministicProtocol(bid);
            detTxnProcessor.AckBatchCommit(bid);

            // send ACKs to local coordinators
            var curScheduleMap = bidToSubBatches[bid];
            var coords = coordPerBatchPerSilo[bid];
            foreach (var item in curScheduleMap)
            {
                var localCoordID = coords[item.Key];
                var dest = GrainFactory.GetGrain<ILocalCoordGrain>(localCoordID);
                _ = dest.AckGlobalBatchCommit(bid);
            }

            // garbage collection
            bidToSubBatches.Remove(bid);
            coordPerBatchPerSilo.Remove(bid);
            expectedAcksPerBatch.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            await detTxnProcessor.WaitBatchCommit(bid);
        }

        public Task SpawnGlobalCoordGrain()
        {
            detTxnProcessor.Init();
            nonDetTxnProcessor.Init();

            var neighborID = GlobalCoordGrainPlacementHelper.MapCoordIDToNeighborID(myID);
            neighborCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(neighborID);

            loggerGroup.GetLoggingProtocol(myID, out log);
            
            batchSizeInMSecs = Constants.batchSizeInMSecsBasic;
            for (int i = Constants.numSilo; i > 2; i /= 2) batchSizeInMSecs *= Constants.scaleSpeed;
            timeOfBatchGeneration = DateTime.Now;

            Console.WriteLine($"Global coord {myID} initialize logging {Constants.loggingType}, batch size = {Helper.ChangeFormat(batchSizeInMSecs, 0)}ms");
            return Task.CompletedTask;
        }
    }
}