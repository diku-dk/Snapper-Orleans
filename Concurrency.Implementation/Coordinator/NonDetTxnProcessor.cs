using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.Coordinator
{
    public class NonDetTxnProcessor
    {
        readonly int myID;
        int numReservedTid;
        long nextAvailableTid;
        float smoothingFactor;
        List<TaskCompletionSource<long>> nonDetRequests;    // int: tid assigned to the ACT

        public NonDetTxnProcessor(int myID)
        {
            this.myID = myID;
            Init();
            nonDetRequests = new List<TaskCompletionSource<long>>();
        }

        public void CheckGC()
        {
            if (nonDetRequests.Count != 0) Console.WriteLine($"GlobalCoord {myID}: nonDetRequests.Count = {nonDetRequests.Count}");
        }

        public void Init()
        {
            numReservedTid = 0;
            nextAvailableTid = -1;
            smoothingFactor = 0.5f;
        }

        public async Task<long> NewNonDet()
        {
            if (numReservedTid > 0)
            {
                numReservedTid--;
                return nextAvailableTid++;
            }

            var promise = new TaskCompletionSource<long>();
            nonDetRequests.Add(promise);
            await promise.Task;
            return promise.Task.Result;
        }

        public void EmitNonDetTransactions(BasicToken token)
        {
            var waitingTxns = nonDetRequests.Count; 

            // assign tids to waited ACTs
            foreach (var txn in nonDetRequests) txn.SetResult(++token.lastEmitTid);
            nonDetRequests.Clear();

            // reset the number of reserved tids
            numReservedTid = (int)(smoothingFactor * waitingTxns + (1 - smoothingFactor) * numReservedTid);
            nextAvailableTid = token.lastEmitTid + 1;

            token.lastEmitTid += numReservedTid;    // reserve range: [nextAvailableTid, nextAvailableTid + numReservedTid - 1]
        }
    }
}