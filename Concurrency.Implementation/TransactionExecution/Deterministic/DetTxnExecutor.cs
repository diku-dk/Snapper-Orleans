using System;
using Utilities;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Coordinator;
using Orleans;
using System.Diagnostics;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.Logging;
using System.Runtime.Serialization;
using MessagePack;

namespace Concurrency.Implementation.TransactionExecution
{
    public class DetTxnExecutor<TState> where TState : ICloneable, ISerializable
    {
        // grain basic info
        readonly int myID;
        readonly int siloID;
        
        // transaction execution
        TransactionScheduler myScheduler;
        ITransactionalState<TState> state;
        ILoggingProtocol log;

        // local and global coordinators
        readonly int myLocalCoordID;
        readonly ICoordMap coordMap;
        readonly ILocalCoordGrain myLocalCoord;
        readonly IGlobalCoordGrain myGlobalCoord;                               // use this coord to get tid for global transactions

        // PACT execution
        Dictionary<int, TaskCompletionSource<bool>> localBtchInfoPromise;       // key: local bid, use to check if the SubBatch has arrived or not
        Dictionary<int, BasicFuncResult> detFuncResults;                        // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times
        
        // only for global PACT
        Dictionary<int, int> globalBidToLocalBid;
        Dictionary<int, Dictionary<int, int>> globalTidToLocalTidPerBatch;      // key: global bid, <global tid, local tid>
        Dictionary<int, TaskCompletionSource<bool>> globalBtchInfoPromise;      // key: global bid, use to check if the SubBatch has arrived or not

        public void CheckGC()
        {
            if (localBtchInfoPromise.Count != 0) Console.WriteLine($"DetTxnExecutor: localBtchInfoPromise.Count = {localBtchInfoPromise.Count}");
            if (detFuncResults.Count != 0) Console.WriteLine($"DetTxnExecutor: detFuncResults.Count = {detFuncResults.Count}");
            if (globalBidToLocalBid.Count != 0) Console.WriteLine($"DetTxnExecutor: globalBidToLocalBid.Count = {globalBidToLocalBid.Count}");
            if (globalTidToLocalTidPerBatch.Count != 0) Console.WriteLine($"DetTxnExecutor: globalTidToLocalTidPerBatch.Count = {globalTidToLocalTidPerBatch.Count}");
            if (globalBtchInfoPromise.Count != 0) Console.WriteLine($"DetTxnExecutor: globalBtchInfoPromise.Count = {globalBtchInfoPromise.Count}");
        }

        public DetTxnExecutor(
            int myID,
            int siloID, 
            ICoordMap coordMap,
            int myLocalCoordID,
            ILocalCoordGrain myLocalCoord,
            IGlobalCoordGrain myGlobalCoord,
            IGrainFactory myGrainFactory,
            TransactionScheduler myScheduler,
            ITransactionalState<TState> state,
            ILoggingProtocol log)
        {
            this.myID = myID;
            this.siloID = siloID;
            this.coordMap = coordMap;
            this.myLocalCoordID = myLocalCoordID;
            this.myLocalCoord = myLocalCoord;
            this.myGlobalCoord = myGlobalCoord;
            this.myScheduler = myScheduler;
            this.state = state;
            this.log = log;

            localBtchInfoPromise = new Dictionary<int, TaskCompletionSource<bool>>();
            detFuncResults = new Dictionary<int, BasicFuncResult>();
            globalBidToLocalBid = new Dictionary<int, int>();
            globalTidToLocalTidPerBatch = new Dictionary<int, Dictionary<int, int>>();
            globalBtchInfoPromise = new Dictionary<int, TaskCompletionSource<bool>>();
        }

        // int: the highestCommittedBid get from local coordinator
        public async Task<Tuple<int, TransactionContext>> GetDetContext(List<int> grainList, List<string> grainClassName)
        {
            if (Constants.multiSilo && Constants.hierarchicalCoord)
            {
                // check if the transaction will access multiple silos
                var siloList = new List<int>();
                var grainListPerSilo = new Dictionary<int, List<int>>();
                var grainNamePerSilo = new Dictionary<int, List<string>>();
                for (int i = 0; i < grainList.Count; i++)
                {
                    var grainID = grainList[i];
                    var siloID = TransactionExecutionGrainPlacementHelper.MapGrainIDToSilo(grainID);
                    if (grainListPerSilo.ContainsKey(siloID) == false)
                    {
                        siloList.Add(siloID);
                        grainListPerSilo.Add(siloID, new List<int>());
                        grainNamePerSilo.Add(siloID, new List<string>());
                    }

                    grainListPerSilo[siloID].Add(grainID);
                    grainNamePerSilo[siloID].Add(grainClassName[i]);
                }

                if (siloList.Count != 1)
                {
                    // get global tid from global coordinator
                    var globalInfo = await myGlobalCoord.NewTransaction(siloList);
                    var globalTid = globalInfo.Item1.tid;
                    var globalBid = globalInfo.Item1.bid;
                    var siloIDToLocalCoordID = globalInfo.Item2;
                    
                    // send corresponding grainAccessInfo to local coordinators in different silos
                    Debug.Assert(grainListPerSilo.ContainsKey(siloID));
                    Task<TransactionRegistInfo> task = null;
                    for (int i = 0; i < siloList.Count; i++)
                    {
                        var siloID = siloList[i];
                        Debug.Assert(siloIDToLocalCoordID.ContainsKey(siloID));
                        var coordID = siloIDToLocalCoordID[siloID];

                        // get local tid, bid from local coordinator
                        var localCoord = coordMap.GetLocalCoord(coordID);
                        if (siloID == this.siloID) task = localCoord.NewGlobalTransaction(globalBid, globalTid, grainListPerSilo[siloID], grainNamePerSilo[siloID]);
                        else _ = localCoord.NewGlobalTransaction(globalBid, globalTid, grainListPerSilo[siloID], grainNamePerSilo[siloID]);
                    }

                    Debug.Assert(task != null);
                    var localInfo = await task;
                    var cxt1 = new TransactionContext(localInfo.bid, localInfo.tid, globalBid, globalTid);
                    return new Tuple<int, TransactionContext>(-1, cxt1) ;
                }
            }

            var info = await myLocalCoord.NewTransaction(grainList, grainClassName);
            var cxt2 = new TransactionContext(info.tid, info.bid);
            return new Tuple<int, TransactionContext>(info.highestCommittedBid, cxt2);
        }

        public async Task WaitForTurn(TransactionContext cxt)
        {
            // check if it is a global PACT
            if (cxt.globalBid != -1)
            {
                // wait until the SubBatch has arrived this grain
                if (globalBtchInfoPromise.ContainsKey(cxt.globalBid) == false)
                    globalBtchInfoPromise.Add(cxt.globalBid, new TaskCompletionSource<bool>());
                await globalBtchInfoPromise[cxt.globalBid].Task;

                // need to map global info to the corresponding local tid and bid
                cxt.localBid = globalBidToLocalBid[cxt.globalBid];
                cxt.localTid = globalTidToLocalTidPerBatch[cxt.globalBid][cxt.globalTid];
            }
            else
            {
                // wait until the SubBatch has arrived this grain
                if (localBtchInfoPromise.ContainsKey(cxt.localBid) == false)
                    localBtchInfoPromise.Add(cxt.localBid, new TaskCompletionSource<bool>());
                await localBtchInfoPromise[cxt.localBid].Task;
            }
            
            Debug.Assert(detFuncResults.ContainsKey(cxt.localTid) == false);
            detFuncResults.Add(cxt.localTid, new BasicFuncResult());
            await myScheduler.WaitForTurn(cxt.localBid, cxt.localTid);
        }

        public async Task FinishExecuteDetTxn(TransactionContext cxt)
        {
            var coordID = myScheduler.AckComplete(cxt.localBid, cxt.localTid);
            if (coordID != -1)   // the current batch has completed on this grain
            {
                // TODO: only writer transaction needs to persist the updated grain state
                if (log != null)
                {
                    var data = MessagePackSerializer.Serialize(state.GetCommittedState(cxt.localBid));
                    await log.HandleOnCompleteInDeterministicProtocol(data, cxt.localBid, coordID);
                } 

                localBtchInfoPromise.Remove(cxt.localBid);
                if (cxt.globalBid != -1)
                {
                    globalBidToLocalBid.Remove(cxt.globalBid);
                    globalTidToLocalTidPerBatch.Remove(cxt.globalBid);
                    globalBtchInfoPromise.Remove(cxt.globalBid);
                }

                myScheduler.scheduleInfo.CompleteDetBatch(cxt.localBid);

                var coord = coordMap.GetLocalCoord(coordID);
                _ = coord.AckBatchCompletion(cxt.localBid);
            }
        }

        /// <summary> Call this interface to emit a SubBatch from a local coordinator to a grain </summary>
        public void BatchArrive(LocalSubBatch batch)
        {
            if (localBtchInfoPromise.ContainsKey(batch.bid) == false)
                localBtchInfoPromise.Add(batch.bid, new TaskCompletionSource<bool>());
            localBtchInfoPromise[batch.bid].SetResult(true);

            // register global info mapping if necessary
            if (batch.globalBid != -1)
            {
                globalBidToLocalBid.Add(batch.globalBid, batch.bid);
                globalTidToLocalTidPerBatch.Add(batch.globalBid, batch.globalTidToLocalTid);

                if (globalBtchInfoPromise.ContainsKey(batch.globalBid) == false)
                    globalBtchInfoPromise.Add(batch.globalBid, new TaskCompletionSource<bool>());
                globalBtchInfoPromise[batch.globalBid].SetResult(true);
            }
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public TState GetState(int tid, AccessMode mode)
        {
            if (mode == AccessMode.Read)
            {
                detFuncResults[tid].isNoOpOnGrain = false;
                detFuncResults[tid].isReadOnlyOnGrain = true;
            }
            else
            {
                detFuncResults[tid].isNoOpOnGrain = false;
                detFuncResults[tid].isReadOnlyOnGrain = false;
            }
            return state.DetOp();
        }

        public async Task<TransactionResult> CallGrain(TransactionContext cxt, FunctionCall call, ITransactionExecutionGrain grain)
        {
            var resultObj = await grain.ExecuteDet(call, cxt);
            return new TransactionResult(resultObj);
        }

        public void CleanUp(int tid)
        {
            detFuncResults.Remove(tid);
        }
    }
}