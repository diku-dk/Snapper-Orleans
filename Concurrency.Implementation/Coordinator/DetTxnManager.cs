using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    public class DetTxnManager
    {
        readonly int myID;
        readonly bool isGlobalCoord;

        List<List<int>> detRequests;
        List<TaskCompletionSource<Tuple<int, int>>> detRequestPromise;        // <bid, tid>
        // only for global batches
        List<List<int>> coordListPerTxn;

        Dictionary<int, int> bidToLastBid;
        Dictionary<int, int> bidToLastCoordID;
        Dictionary<int, int> expectedAcksPerBatch;
        Dictionary<int, Dictionary<int, SubBatch>> batchSchedulePerService;   // <bid, GrainID, batch schedule>
        // only for global batches
        Dictionary<int, Dictionary<int, int>> coordPerBatchPerSilo;

        public DetTxnManager(
            int myID,
            Dictionary<int, int> bidToLastBid,
            Dictionary<int, int> bidToLastCoordID,
            Dictionary<int, int> expectedAcksPerBatch,
            Dictionary<int, Dictionary<int, SubBatch>> batchSchedulePerService,
            Dictionary<int, Dictionary<int, int>> coordPerBatchPerSilo = null)
        {
            this.myID = myID;
            this.bidToLastBid = bidToLastBid;
            this.bidToLastCoordID = bidToLastCoordID;
            this.expectedAcksPerBatch = expectedAcksPerBatch;
            this.batchSchedulePerService = batchSchedulePerService;
            if (coordPerBatchPerSilo != null)
            {
                isGlobalCoord = true;
                this.coordPerBatchPerSilo = coordPerBatchPerSilo;
                coordListPerTxn = new List<List<int>>();
            }
            else isGlobalCoord = false;
            detRequests = new List<List<int>>();
            detRequestPromise = new List<TaskCompletionSource<Tuple<int, int>>>();
        }

        public void CheckGC()
        {
            if (detRequests.Count != 0) Console.WriteLine($"GlobalCoord {myID}: detRequests.Count = {detRequests.Count}");
            if (detRequestPromise.Count != 0) Console.WriteLine($"GlobalCoord {myID}: detRequestPromise.Count = {detRequestPromise.Count}");
            if (coordListPerTxn.Count != 0) Console.WriteLine($"GlobalCoord {myID}: coordListPerTxn.Count = {coordListPerTxn.Count}");
        }

        // for PACT
        public async Task<Tuple<int, int>> NewDet(List<int> serviceList, List<int> coordList = null)
        {
            if (isGlobalCoord) coordListPerTxn.Add(coordList);
            detRequests.Add(serviceList);
            var promise = new TaskCompletionSource<Tuple<int, int>>();
            detRequestPromise.Add(promise);
            await promise.Task;
            return new Tuple<int, int>(promise.Task.Result.Item1, promise.Task.Result.Item2);
        }

        public int GenerateBatch(BasicToken token)
        {
            if (detRequests.Count == 0) return -1;

            // assign bid and tid to waited PACTs
            var curBatchID = token.lastEmitTid + 1;
            for (int i = 0; i < detRequests.Count; i++)
            {
                var tid = ++token.lastEmitTid;
                detRequestPromise[i].SetResult(new Tuple<int, int>(curBatchID, tid));

                if (isGlobalCoord) GnerateSchedulePerService(tid, curBatchID, detRequests[i], coordListPerTxn[i]);
                else GnerateSchedulePerService(tid, curBatchID, detRequests[i]);
            }
            UpdateToken(token, curBatchID);

            detRequests.Clear();
            detRequestPromise.Clear();
            if (isGlobalCoord) coordListPerTxn.Clear();
            return curBatchID;
        }

        public void GnerateSchedulePerService(int tid, int curBatchID, List<int> serviceList, List<int> coordList = null)
        {
            if (batchSchedulePerService.ContainsKey(curBatchID) == false)
            {
                batchSchedulePerService.Add(curBatchID, new Dictionary<int, SubBatch>());
                if (isGlobalCoord) coordPerBatchPerSilo.Add(curBatchID, new Dictionary<int, int>());
            }
                
            var schedule = batchSchedulePerService[curBatchID];

            for (int i = 0; i < serviceList.Count; i++)
            {
                var serviceID = serviceList[i];
                if (schedule.ContainsKey(serviceID) == false)
                {
                    schedule.Add(serviceID, new SubBatch(curBatchID, myID));
                    if (isGlobalCoord) coordPerBatchPerSilo[curBatchID].Add(serviceID, coordList[i]);
                }

                schedule[serviceID].txnList.Add(tid);
            }
        }

        public void UpdateToken(BasicToken token, int curBatchID)
        {
            var curScheduleMap = batchSchedulePerService[curBatchID];
            expectedAcksPerBatch.Add(curBatchID, curScheduleMap.Count);

            // update the last batch ID for each grain accessed by this batch
            foreach (var grain in curScheduleMap)
            {
                var schedule = grain.Value;
                if (token.lastBidPerService.ContainsKey(grain.Key)) schedule.lastBid = token.lastBidPerService[grain.Key];
                else schedule.lastBid = -1;
                Debug.Assert(schedule.bid > schedule.lastBid);
                token.lastBidPerService[grain.Key] = schedule.bid;
            }
            bidToLastBid.Add(curBatchID, token.lastEmitBid);
            if (token.lastEmitBid != -1) bidToLastCoordID.Add(curBatchID, token.lastCoordID);
            token.lastEmitBid = curBatchID;
            token.lastCoordID = myID;
        }

        public void GarbageCollectTokenInfo(int highestCommittedBid, BasicToken token)
        {
            if (highestCommittedBid > token.highestCommittedBid)
            {
                var expiredGrains = new HashSet<int>();
                foreach (var item in token.lastBidPerService)  // only when last batch is already committed, the next emmitted batch can have its lastBid = -1 again
                    if (item.Value <= highestCommittedBid) expiredGrains.Add(item.Key);
                foreach (var item in expiredGrains) token.lastBidPerService.Remove(item);
            }
        }
    }
}