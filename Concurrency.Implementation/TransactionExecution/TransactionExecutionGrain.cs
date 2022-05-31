using System;
using Orleans;
using Utilities;
using Orleans.Concurrency;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.TransactionExecution.Nondeterministic;
using System.Runtime.Serialization;

namespace Concurrency.Implementation.TransactionExecution
{
    [Reentrant]
    [TransactionExecutionGrainPlacementStrategy]
    public abstract class TransactionExecutionGrain<TState> : Grain, ITransactionExecutionGrain where TState : ICloneable, ISerializable, new()
    {
        // grain basic info
        int myID;
        int mySiloID;
        readonly ICoordMap coordMap;
        readonly string myClassName;
        static int myLocalCoordID;
        static ILocalCoordGrain myLocalCoord;   // use this coord to get tid for local transactions
        static IGlobalCoordGrain myGlobalCoord;

        // transaction execution
        ILoggingProtocol log;
        readonly ILoggerGroup loggerGroup;
        TransactionScheduler myScheduler;
        ITransactionalState<TState> state;

        // PACT execution
        DetTxnExecutor<TState> detTxnExecutor;
        Dictionary<int, TaskCompletionSource<bool>> batchCommit;                // key: local bid

        // ACT execution
        Dictionary<int, int> coordinatorMap;
        NonDetTxnExecutor<TState> nonDetTxnExecutor;
        NonDetCommitter<TState> nonDetCommitter;

        // garbage collection
        int highestCommittedLocalBid;

        public TransactionExecutionGrain(ILoggerGroup loggerGroup, ICoordMap coordMap, string myClassName)
        {
            this.loggerGroup = loggerGroup;
            this.coordMap = coordMap;
            this.myClassName = myClassName;
        }

        public Task CheckGC()
        {
            state.CheckGC();
            myScheduler.CheckGC();
            detTxnExecutor.CheckGC();
            nonDetTxnExecutor.CheckGC();
            nonDetCommitter.CheckGC();
            if (batchCommit.Count != 0) Console.WriteLine($"TransactionExecutionGrain: batchCommit.Count = {batchCommit.Count}");
            if (coordinatorMap.Count != 0) Console.WriteLine($"TransactionExecutionGrain: coordinatorMap.Count = {coordinatorMap.Count}");
             
            return Task.CompletedTask;
        }

        public override Task OnActivateAsync()
        {
            highestCommittedLocalBid = -1;

            // grain basic info
            myID = (int)this.GetPrimaryKeyLong();
            mySiloID = TransactionExecutionGrainPlacementHelper.MapGrainIDToSilo(myID);

            // transaction execution
            loggerGroup.GetLoggingProtocol(myID, out log);
            myScheduler = new TransactionScheduler(myID);
            state = new HybridState<TState>();
            batchCommit = new Dictionary<int, TaskCompletionSource<bool>>();
            coordinatorMap = new Dictionary<int, int>();

            // set up local and global coordinator info
            if (Constants.multiSilo)
            {
                if (Constants.hierarchicalCoord)
                {
                    var localCoordIndex = Helper.MapGrainIDToServiceID(myID, Constants.numLocalCoordPerSilo);
                    myLocalCoordID = LocalCoordGrainPlacementHelper.MapCoordIndexToCoordID(localCoordIndex, mySiloID);
                    myLocalCoord = GrainFactory.GetGrain<ILocalCoordGrain>(myLocalCoordID);

                    var globalCoordID = Helper.MapGrainIDToServiceID(myID, Constants.numGlobalCoord);
                    myGlobalCoord = GrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID);
                }
                else   // all local coordinators are put in a separate silo
                {
                    myLocalCoordID = Helper.MapGrainIDToServiceID(myID, Constants.numGlobalCoord);
                    myLocalCoord = GrainFactory.GetGrain<ILocalCoordGrain>(myLocalCoordID);
                }
            }
            else   // single silo deployment
            {
                myLocalCoordID = Helper.MapGrainIDToServiceID(myID, Constants.numLocalCoordPerSilo);
                myLocalCoord = GrainFactory.GetGrain<ILocalCoordGrain>(myLocalCoordID);
            }

            detTxnExecutor = new DetTxnExecutor<TState>(
                myID,
                mySiloID,
                coordMap,
                myLocalCoordID,
                myLocalCoord,
                myGlobalCoord,
                GrainFactory,
                myScheduler,
                state,
                log);

            nonDetTxnExecutor = new NonDetTxnExecutor<TState>(
                myID,
                mySiloID,
                myClassName,
                myLocalCoord,
                myGlobalCoord,
                myScheduler,
                state);

            nonDetCommitter = new NonDetCommitter<TState>(
                myID,
                coordinatorMap,
                state,
                log,
                GrainFactory);

            return Task.CompletedTask;
        }

        // Notice: the current implementation assumes each actor will be accessed at most once
        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<int> grainAccessInfo, List<string> grainClassName)
        {
            var receiveTxnTime = DateTime.Now;
            var cxtInfo = await detTxnExecutor.GetDetContext(grainAccessInfo, grainClassName);
            var cxt = cxtInfo.Item2;

            if (highestCommittedLocalBid < cxtInfo.Item1)
            {
                highestCommittedLocalBid = cxtInfo.Item1;
                myScheduler.AckBatchCommit(highestCommittedLocalBid);
            }

            // execute PACT
            var call = new FunctionCall(startFunc, funcInput, GetType());
            var res = await ExecuteDet(call, cxt);
            var finishExeTime = DateTime.Now;
            var startExeTime = res.Item2;
            var resultObj = res.Item1;

            // wait for this batch to commit
            await WaitForBatchCommit(cxt.localBid);

            var commitTime = DateTime.Now;
            var txnResult = new TransactionResult(resultObj);
            txnResult.prepareTime = (startExeTime - receiveTxnTime).TotalMilliseconds;
            txnResult.executeTime = (finishExeTime - startExeTime).TotalMilliseconds;
            txnResult.commitTime = (commitTime - finishExeTime).TotalMilliseconds;
            return txnResult;
        }

        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            var receiveTxnTime = DateTime.Now;
            var cxtInfo = await nonDetTxnExecutor.GetNonDetContext();
            if (highestCommittedLocalBid < cxtInfo.Item1)
            {
                highestCommittedLocalBid = cxtInfo.Item1;
                myScheduler.AckBatchCommit(highestCommittedLocalBid);
            }
            var cxt = cxtInfo.Item2;

            // execute ACT
            var call = new FunctionCall(startFunc, funcInput, GetType());
            var res1 = await ExecuteNonDet(call, cxt);
            var finishExeTime = DateTime.Now;
            var startExeTime = res1.Item2;
            var funcResult = res1.Item1;

            // check serializability and do 2PC
            var canCommit = !funcResult.exception;
            var res = new TransactionResult(funcResult.resultObj);
            var isPrepared = false;
            if (canCommit)
            {
                var result = nonDetCommitter.CheckSerializability(funcResult.globalScheduleInfo, funcResult.scheduleInfoPerSilo);
                canCommit = result.Item1;
                if (canCommit)
                {
                    isPrepared = true;
                    canCommit = await nonDetCommitter.CoordPrepare(cxt.globalTid, funcResult.grainOpInfo);
                }
                else
                {
                    if (result.Item2) res.Exp_Serializable = true;
                    else res.Exp_NotSureSerializable = true;
                }
            }
            else res.Exp_Deadlock |= funcResult.Exp_Deadlock;  // when deadlock = false, exception may from RW conflict

            if (canCommit) await nonDetCommitter.CoordCommit(cxt.globalTid, funcResult);
            else
            {
                res.exception = true;
                await nonDetCommitter.CoordAbort(cxt.globalTid, funcResult.grainOpInfo, isPrepared);
            }

            // wait for maxBeforeLocalBid in each silo to commit
            if (canCommit)
            {
                foreach (var siloInfo in funcResult.grainWithMaxBeforeLocalBidPerSilo)
                {
                    var siloID = siloInfo.Key;
                    var grainID = siloInfo.Value;
                    var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.Item1, grainID.Item2);
                    await grain.WaitForBatchCommit(funcResult.scheduleInfoPerSilo[siloID].maxBeforeBid);
                }
            }
            var commitTime = DateTime.Now;
            res.prepareTime = (startExeTime - receiveTxnTime).TotalMilliseconds;
            res.executeTime = (finishExeTime - startExeTime).TotalMilliseconds;
            res.commitTime = (commitTime - finishExeTime).TotalMilliseconds;
            return res;
        }

        /// <summary> Call this interface to emit a SubBatch from a local coordinator to a grain </summary>
        public Task ReceiveBatchSchedule(LocalSubBatch batch)
        {
            // do garbage collection for committed local batches
            if (highestCommittedLocalBid < batch.highestCommittedBid)
            {
                highestCommittedLocalBid = batch.highestCommittedBid;
                myScheduler.AckBatchCommit(highestCommittedLocalBid);
            }
            batchCommit.Add(batch.bid, new TaskCompletionSource<bool>());

            // register the local SubBatch info
            myScheduler.RegisterBatch(batch, batch.globalBid, highestCommittedLocalBid);
            detTxnExecutor.BatchArrive(batch);
            
            return Task.CompletedTask;
        }

        /// <summary> When commit an ACT, call this interface to wait for a specific local batch to commit </summary>
        public async Task WaitForBatchCommit(int bid)
        {
            if (highestCommittedLocalBid >= bid) return;
            await batchCommit[bid].Task;
        }

        /// <summary> A local coordinator calls this interface to notify the commitment of a local batch </summary>
        public Task AckBatchCommit(int bid)
        {
            if (highestCommittedLocalBid < bid)
            {
                highestCommittedLocalBid = bid;
                myScheduler.AckBatchCommit(highestCommittedLocalBid);
            }
            batchCommit[bid].SetResult(true);
            batchCommit.Remove(bid);
            //myScheduler.AckBatchCommit(highestCommittedBid);
            return Task.CompletedTask;
        }

        /// <summary> When execute a transaction on the grain, call this interface to read / write grain state </summary>
        public async Task<TState> GetState(TransactionContext cxt, AccessMode mode)
        {
            var isDet = cxt.localBid != -1;
            if (isDet) return detTxnExecutor.GetState(cxt.localTid, mode);
            else return await nonDetTxnExecutor.GetState(cxt.globalTid, mode);
        }

        public async Task<Tuple<object, DateTime>> ExecuteDet(FunctionCall call, TransactionContext cxt)
        {
            await detTxnExecutor.WaitForTurn(cxt);
            var time = DateTime.Now;
            var txnRes = await InvokeFunction(call, cxt);   // execute the function call;
            await detTxnExecutor.FinishExecuteDetTxn(cxt);
            detTxnExecutor.CleanUp(cxt.localTid);
            return new Tuple<object, DateTime>(txnRes.resultObj, time);
        }

        public async Task<Tuple<NonDetFuncResult, DateTime>> ExecuteNonDet(FunctionCall call, TransactionContext cxt)
        {
            var canExecute = await nonDetTxnExecutor.WaitForTurn(cxt.globalTid);
            var time = DateTime.Now;
            if (canExecute == false)
            {
                var funcResult = new NonDetFuncResult();
                funcResult.Exp_Deadlock = true;
                funcResult.exception = true;
                nonDetTxnExecutor.CleanUp(cxt.globalTid);
                return new Tuple<NonDetFuncResult, DateTime>(funcResult, time);
            }
            else
            {
                var exception = false;
                Object resultObj = null;
                try
                {
                    var txnRes = await InvokeFunction(call, cxt);
                    resultObj = txnRes.resultObj;
                }
                catch (Exception)
                {
                    // exceptions thrown from GetState will be caught here
                    exception = true;
                }
                var funcResult = nonDetTxnExecutor.UpdateExecutionResult(cxt.globalTid, highestCommittedLocalBid);
                if (resultObj != null) funcResult.SetResultObj(resultObj);
                nonDetTxnExecutor.CleanUp(cxt.globalTid);
                if (exception) CleanUp(cxt.globalTid);
                return new Tuple<NonDetFuncResult, DateTime>(funcResult, time);
            }
        }

        async Task<TransactionResult> InvokeFunction(FunctionCall call, TransactionContext cxt)
        {
            if (cxt.localBid == -1)
            {
                Debug.Assert(coordinatorMap.ContainsKey(cxt.globalTid) == false);
                coordinatorMap.Add(cxt.globalTid, cxt.nonDetCoordID);
            }
            var mi = call.grainClassName.GetMethod(call.funcName);
            var t = (Task<TransactionResult>)mi.Invoke(this, new object[] { cxt, call.funcInput });
            return await t;
        }

        /// <summary> When execute a transaction, call this interface to make a cross-grain function invocation </summary>
        public Task<TransactionResult> CallGrain(TransactionContext cxt, int grainID, string grainNameSpace, FunctionCall call)
        {
            var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID, grainNameSpace);
            var isDet = cxt.localBid != -1;
            if (isDet) return detTxnExecutor.CallGrain(cxt, call, grain);
            else return nonDetTxnExecutor.CallGrain(cxt, call, grain);
        }

        public async Task<bool> Prepare(int tid, bool isReader)
        {
            var vote = await nonDetCommitter.Prepare(tid, isReader);
            if (isReader) CleanUp(tid);
            return vote;
        }

        // only writer grain needs 2nd phase of 2PC
        public async Task Commit(int tid, int maxBeforeLocalBid, int maxBeforeGlobalBid)   
        {
            nonDetTxnExecutor.Commit(maxBeforeLocalBid, maxBeforeGlobalBid);
            await nonDetCommitter.Commit(tid);
            CleanUp(tid);
        }

        public Task Abort(int tid)
        {
            nonDetCommitter.Abort(tid);
            CleanUp(tid);
            return Task.CompletedTask;
        }

        void CleanUp(int tid)
        {
            coordinatorMap.Remove(tid);
            myScheduler.scheduleInfo.CompleteNonDetTxn(tid);
        }
    }
}