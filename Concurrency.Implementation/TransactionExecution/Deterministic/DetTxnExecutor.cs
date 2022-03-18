using System;
using Utilities;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.Coordinator;
using Orleans;
using System.Diagnostics;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.Logging;

namespace Concurrency.Implementation.TransactionExecution
{
    public class DetTxnExecutor<TState>
    {
        // grain basic info
        readonly int siloID;

        // transaction execution
        TransactionScheduler myScheduler;
        ITransactionalState<TState> state;
        ILoggingProtocol<TState> log;

        // local and global coordinators
        readonly int myLocalCoordID;
        readonly ILocalCoordGrain myLocalCoord;
        readonly IGlobalCoordGrain myGlobalCoord;                               // use this coord to get tid for global transactions
        readonly Dictionary<int, ILocalCoordGrain> localCoordMap;               // <coordID, local coord>    !!!!!!!!!!!!!!!!!!!!!
        readonly Dictionary<int, IGlobalCoordGrain> globalCoordMap;             // <coordID, global coord>

        // PACT execution
        Dictionary<int, TaskCompletionSource<bool>> localBtchInfoPromise;       // key: local bid, use to check if the SubBatch has arrived or not
        Dictionary<int, BasicFuncResult> detFuncResults;                        // key: local PACT tid, this can only work when a transaction do not concurrently access one grain multiple times
        
        // only for global PACT
        Dictionary<int, int> globalBidToLocalBid;
        Dictionary<int, Dictionary<int, int>> globalTidToLocalTidPerBatch;      // key: global bid, <global tid, local tid>
        Dictionary<int, TaskCompletionSource<bool>> globallocalBtchInfoPromise; // key: global bid, use to check if the SubBatch has arrived or not

        public DetTxnExecutor(
            int siloID, 
            int myLocalCoordID,
            ILocalCoordGrain myLocalCoord,
            IGlobalCoordGrain myGlobalCoord,
            IGrainFactory myGrainFactory,
            TransactionScheduler myScheduler,
            ITransactionalState<TState> state,
            ILoggingProtocol<TState> log)
        {
            this.siloID = siloID;
            this.myLocalCoordID = myLocalCoordID;
            this.myLocalCoord = myLocalCoord;
            this.myGlobalCoord = myGlobalCoord;
            this.myScheduler = myScheduler;
            this.state = state;
            this.log = log;

            // set up local and global coordinator info
            if (Constants.multiSilo)
            {
                if (Constants.hierarchicalCoord)
                {
                    var firstCoordID = LocalCoordGrainPlacementHelper.MapSiloIDToFirstLocalCoordID(siloID);
                    for (int i = 0; i < Constants.numLocalCoordPerSilo; i++)
                    {
                        var ID = i + firstCoordID;
                        var coord = myGrainFactory.GetGrain<ILocalCoordGrain>(ID);
                        localCoordMap.Add(ID, coord);
                    }

                    for (int i = 0; i < Constants.numGlobalCoord; i++)
                    {
                        var coord = myGrainFactory.GetGrain<IGlobalCoordGrain>(i);
                        globalCoordMap.Add(i, coord);
                    }
                }
                else   // all local coordinators are put in a separate silo
                {
                    for (int i = 0; i < Constants.numGlobalCoord; i++)
                    {
                        var coord = myGrainFactory.GetGrain<ILocalCoordGrain>(i);
                        localCoordMap.Add(i, coord);
                    }
                }
            }
            else   // single silo deployment
            {
                for (int i = 0; i < Constants.numLocalCoordPerSilo; i++)
                {
                    var coord = myGrainFactory.GetGrain<ILocalCoordGrain>(i);
                    localCoordMap.Add(i, coord);
                }
            }
        }

        public void CheckGC()
        {
            
        }

        // int: the highestCommittedBid get from local coordinator
        public async Task<Tuple<int, TransactionContext>> GetDetContext(List<int> grainList, List<string> grainClassName)
        {
            if (Constants.multiSilo && Constants.hierarchicalCoord)
            {
                // check if the transaction will access multiple silos
                var siloList = new List<int>();
                var coordList = new List<int>();
                var grainListPerSilo = new Dictionary<int, List<int>>();
                var grainNamePerSilo = new Dictionary<int, List<string>>();
                for (int i = 0; i < grainList.Count; i++)
                {
                    var grainID = grainList[i];
                    var siloID = TransactionExecutionGrainPlacementHelper.MapGrainIDToSilo(grainID);
                    if (grainListPerSilo.ContainsKey(siloID) == false)
                    {
                        siloList.Add(siloID);
                        int coordID;
                        if (siloID == this.siloID) coordID = myLocalCoordID;
                        else coordID = LocalCoordGrainPlacementHelper.MapSiloIDToRandomCoordID(siloID);
                        coordList.Add(coordID);
                        grainListPerSilo.Add(siloID, new List<int>());
                        grainNamePerSilo.Add(siloID, new List<string>());
                    }

                    grainListPerSilo[siloID].Add(grainID);
                    grainNamePerSilo[siloID].Add(grainClassName[i]);
                }

                if (siloList.Count != 1)
                {
                    // get global tid from global coordinator
                    var globalInfo = await myGlobalCoord.NewTransaction(siloList, coordList);
                    var globalTid = globalInfo.tid;

                    // send corresponding grainAccessInfo to local coordinators in different silos
                    Debug.Assert(grainListPerSilo.ContainsKey(siloID));
                    TransactionRegistInfo localInfo = null;
                    for (int i = 0; i < siloList.Count; i++)
                    {
                        var siloID = siloList[i];

                        // get local tid, bid from local coordinator
                        if (siloID == myLocalCoordID)
                            localInfo = await myLocalCoord.NewGlobalTransaction(globalTid, grainListPerSilo[siloID]);
                        else
                        {
                            var coordID = coordList[i];
                            _ = localCoordMap[coordID].NewGlobalTransaction(globalTid, grainListPerSilo[siloID]);
                        }
                    }

                    var cxt1 = new TransactionContext(localInfo.bid, localInfo.tid, globalInfo.bid, globalInfo.tid);
                    return new Tuple<int, TransactionContext>(-1, cxt1) ;
                }
            }

            var info = await myLocalCoord.NewTransaction(grainList, grainClassName);
            var cxt2 = new TransactionContext(info.tid, info.bid);
            return new Tuple<int, TransactionContext>(info.highestCommittedBid, cxt2);
        }

        public async Task<object> ExecuteDet(FunctionCall call, TransactionContext cxt)
        {
            // check if it is a global PACT
            if (cxt.globalBid != -1)
            {
                // wait until the SubBatch has arrived this grain
                if (globallocalBtchInfoPromise.ContainsKey(cxt.globalBid) == false)
                    globallocalBtchInfoPromise.Add(cxt.globalBid, new TaskCompletionSource<bool>());
                await globallocalBtchInfoPromise[cxt.globalBid].Task;

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

            await myScheduler.waitForTurn(cxt.localBid, cxt.localTid);

            // execute the function call;
            var txnResult = await InvokeFunction(call, cxt);
            var funcResult = detFuncResults[cxt.localTid];

            if (myScheduler.ackComplete(cxt.localBid, cxt.localTid))
            {
                // the current batch has completed on this grain
                var coordID = myScheduler.GetCoordID(cxt.localBid);

                // only writer transaction needs to persist the updated grain state
                if (log != null && funcResult.isReadOnlyOnGrain == false)
                    await log.HandleOnCompleteInDeterministicProtocol(state, cxt.localBid, coordID);

                var coord = localCoordMap[coordID];
                _ = coord.AckBatchCompletion(cxt.localBid);
            }
            return txnResult.resultObj;
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

                if (globallocalBtchInfoPromise.ContainsKey(batch.globalBid) == false)
                    globallocalBtchInfoPromise.Add(batch.globalBid, new TaskCompletionSource<bool>());
                globallocalBtchInfoPromise[batch.globalBid].SetResult(true);
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
            return state.detOp();
        }

        public async Task<TransactionResult> CallGrain(TransactionContext cxt, FunctionCall call, ITransactionExecutionGrain grain)
        {
            var resultObj = await grain.ExecuteDet(call, cxt);
            return new TransactionResult(resultObj);
        }

        async Task<TransactionResult> InvokeFunction(FunctionCall call, TransactionContext cxt)
        {
            var mi = call.grainClassName.GetMethod(call.funcName);
            var t = (Task<TransactionResult>)mi.Invoke(this, new object[] { cxt, call.funcInput });
            return await t;
        }
    }
}