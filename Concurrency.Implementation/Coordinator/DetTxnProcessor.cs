using Concurrency.Interface.Coordinator;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    public class DetTxnProcessor
    {
        readonly int myID;
        readonly bool isGlobalCoord;
        public long highestCommittedBid;
        readonly ICoordMap coordMap;

        // transaction processing
        List<List<int>> detRequests;
        List<TaskCompletionSource<Tuple<long, long>>> detRequestPromise;                              // <local bid, local tid>

        // batch processing
        Dictionary<long, long> bidToLastBid;
        Dictionary<long, int> bidToLastCoordID;                                                      // <bid, coordID who emit this bid's lastBid>
        Dictionary<long, int> expectedAcksPerBatch;
        Dictionary<long, Dictionary<int, SubBatch>> bidToSubBatches;                                 // <bid, Service ID, subBatch>
        Dictionary<long, TaskCompletionSource<bool>> batchCommit;
        // only for global batch
        Dictionary<long, Dictionary<int, int>> coordPerBatchPerSilo;                                 // global bid, silo ID, chosen local coord ID

        public DetTxnProcessor(
            int myID,
            ICoordMap coordMap,
            Dictionary<long, int> expectedAcksPerBatch,
            Dictionary<long, Dictionary<int, SubBatch>> bidToSubBatches,
            Dictionary<long, Dictionary<int, int>> coordPerBatchPerSilo = null)
        {
            this.myID = myID;
            this.coordMap = coordMap;
            bidToLastBid = new Dictionary<long, long>();
            bidToLastCoordID = new Dictionary<long, int>();
            this.expectedAcksPerBatch = expectedAcksPerBatch;
            this.bidToSubBatches = bidToSubBatches;
            if (coordPerBatchPerSilo != null)
            {
                isGlobalCoord = true;
                this.coordPerBatchPerSilo = coordPerBatchPerSilo;
            }
            else isGlobalCoord = false;

            Init();
        }

        public void CheckGC()
        {
            if (detRequests.Count != 0) Console.WriteLine($"DetTxnProcessor: detRequests.Count = {detRequests.Count}");
            if (detRequestPromise.Count != 0) Console.WriteLine($"DetTxnProcessor: detRequestPromise.Count = {detRequestPromise.Count}");
            if (batchCommit.Count != 0) Console.WriteLine($"DetTxnProcessor: batchCommit.Count = {batchCommit.Count}");
            if (bidToLastCoordID.Count != 0) Console.WriteLine($"DetTxnProcessor {myID}: bidToLastCoordID.Count = {bidToLastCoordID.Count}");
            if (bidToLastBid.Count != 0) Console.WriteLine($"DetTxnProcessor {myID}: bidToLastBid.Count = {bidToLastBid.Count}");
        }

        public void Init()
        {
            highestCommittedBid = -1;
            detRequests = new List<List<int>>();
            detRequestPromise = new List<TaskCompletionSource<Tuple<long, long>>>();
            batchCommit = new Dictionary<long, TaskCompletionSource<bool>>();
        }

        // for PACT
        public async Task<Tuple<long, long>> NewDet(List<int> serviceList)   // <bid, tid>
        {
            detRequests.Add(serviceList);
            var promise = new TaskCompletionSource<Tuple<long, long>>();
            detRequestPromise.Add(promise);
            await promise.Task;
            return new Tuple<long, long>(promise.Task.Result.Item1, promise.Task.Result.Item2);
        }

        public long GenerateBatch(BasicToken token)
        {
            if (detRequests.Count == 0) return -1;

            // assign bid and tid to waited PACTs
            var curBatchID = token.lastEmitTid + 1;

            for (int i = 0; i < detRequests.Count; i++)
            {
                var tid = ++token.lastEmitTid;
                GnerateSchedulePerService(tid, curBatchID, detRequests[i]);
                detRequestPromise[i].SetResult(new Tuple<long, long>(curBatchID, tid));
            }
            UpdateToken(token, curBatchID, -1);

            detRequests.Clear();
            detRequestPromise.Clear();
            return curBatchID;
        }

        public void GnerateSchedulePerService(long tid, long curBatchID, List<int> serviceList)
        {
            if (bidToSubBatches.ContainsKey(curBatchID) == false)
            {
                bidToSubBatches.Add(curBatchID, new Dictionary<int, SubBatch>());
                if (isGlobalCoord) coordPerBatchPerSilo.Add(curBatchID, new Dictionary<int, int>());
            }

            var serviceIDToSubBatch = bidToSubBatches[curBatchID];

            for (int i = 0; i < serviceList.Count; i++)
            {
                var serviceID = serviceList[i];
                if (serviceIDToSubBatch.ContainsKey(serviceID) == false)
                {
                    serviceIDToSubBatch.Add(serviceID, new SubBatch(curBatchID, myID));
                    if (isGlobalCoord)
                    {
                        // randomly choose a local coord as the coordinator for this batch on that silo
                        var chosenCoordID = LocalCoordGrainPlacementHelper.MapSiloIDToRandomCoordID(serviceID);
                        coordPerBatchPerSilo[curBatchID].Add(serviceID, chosenCoordID);
                    }
                }

                serviceIDToSubBatch[serviceID].txnList.Add(tid);
            }
        }

        public void UpdateToken(BasicToken token, long curBatchID, long globalBid)
        {
            var serviceIDToSubBatch = bidToSubBatches[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, serviceIDToSubBatch.Count);

            // update the last batch ID for each service accessed by this batch
            foreach (var serviceInfo in serviceIDToSubBatch)
            {
                var serviceID = serviceInfo.Key;
                var subBatch = serviceInfo.Value;

                if (token.lastBidPerService.ContainsKey(serviceID))
                {
                    subBatch.lastBid = token.lastBidPerService[serviceID];
                    if (isGlobalCoord == false) subBatch.lastGlobalBid = token.lastGlobalBidPerGrain[serviceID];
                }
                // else, the default value is -1

                Debug.Assert(subBatch.bid > subBatch.lastBid);
                token.lastBidPerService[serviceID] = subBatch.bid;
                if (isGlobalCoord == false) token.lastGlobalBidPerGrain[serviceID] = globalBid;
            }
            bidToLastBid.Add(curBatchID, token.lastEmitBid);
            if (token.lastEmitBid != -1) bidToLastCoordID.Add(curBatchID, token.lastCoordID);
            token.lastEmitBid = curBatchID;
            token.isLastEmitBidGlobal = globalBid != -1;
            token.lastCoordID = myID;
        }

        public void GarbageCollectTokenInfo(BasicToken token)
        {
            Debug.Assert(isGlobalCoord == false);
            var expiredGrains = new HashSet<int>();

            // only when last batch is already committed, the next emmitted batch can have its lastBid = -1 again
            foreach (var item in token.lastBidPerService)
                if (item.Value <= highestCommittedBid) expiredGrains.Add(item.Key);
            foreach (var item in expiredGrains)
            {
                token.lastBidPerService.Remove(item);
                token.lastGlobalBidPerGrain.Remove(item);
            } 

            token.highestCommittedBid = highestCommittedBid;
        }

        public async Task WaitPrevBatchToCommit(long bid)
        {
            var lastBid = bidToLastBid[bid];
            bidToLastBid.Remove(bid);

            if (highestCommittedBid < lastBid)
            {
                var coord = bidToLastCoordID[bid];
                if (coord == myID) await WaitBatchCommit(lastBid);
                else
                {
                    if (isGlobalCoord)
                    {
                        var lastCoord = coordMap.GetGlobalCoord(coord);
                        await lastCoord.WaitBatchCommit(lastBid);
                    }
                    else
                    {
                        var lastCoord = coordMap.GetLocalCoord(coord);
                        await lastCoord.WaitBatchCommit(lastBid);
                    }
                }
            }
            else Debug.Assert(highestCommittedBid == lastBid);

            if (bidToLastCoordID.ContainsKey(bid)) bidToLastCoordID.Remove(bid);
        }

        public async Task WaitBatchCommit(long bid)
        {
            if (highestCommittedBid == bid) return;
            if (batchCommit.ContainsKey(bid) == false) batchCommit.Add(bid, new TaskCompletionSource<bool>());
            await batchCommit[bid].Task;
        }

        public void AckBatchCommit(long bid)
        {
            highestCommittedBid = Math.Max(bid, highestCommittedBid);
            if (batchCommit.ContainsKey(bid))
            {
                batchCommit[bid].SetResult(true);
                batchCommit.Remove(bid);
            }
        }
    }
}