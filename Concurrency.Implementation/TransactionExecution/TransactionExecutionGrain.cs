﻿using System;
using Orleans;
using Utilities;
using Orleans.Concurrency;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using Concurrency.Implementation.Logging;
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
        int siloID;
        string myClassName;
        static IGrainFactory myGrainFactory;
        static int myLocalCoordID;
        static ILocalCoordGrain myLocalCoord;   // use this coord to get tid for local transactions
        static IGlobalCoordGrain myGlobalCoord;

        // garbage collection
        int highestCommittedBid;                // local bid

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

        public TransactionExecutionGrain(ILoggerGroup loggerGroup, string myClassName)
        {
            this.loggerGroup = loggerGroup;
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
            highestCommittedBid = -1;
            
            // grain basic info
            myGrainFactory = GrainFactory;
            myID = (int)this.GetPrimaryKeyLong();
            siloID = TransactionExecutionGrainPlacementHelper.MapGrainIDToSilo(myID);

            // transaction execution
            if (Constants.loggingType == LoggingType.LOGGER)
            {
                var loggerID = Helper.MapGrainIDToServiceID(myID, Constants.numLoggerPerSilo);
                var logger = loggerGroup.GetSingleton(loggerID);
                log = new LoggingProtocol(GetType().ToString(), myID, logger);
            }
            else if (Constants.loggingType == LoggingType.ONGRAIN)
                log = new LoggingProtocol(GetType().ToString(), myID);
            else log = null;

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
                    myLocalCoordID = LocalCoordGrainPlacementHelper.MapCoordIndexToCoordID(localCoordIndex, siloID);
                    myLocalCoord = GrainFactory.GetGrain<ILocalCoordGrain>(myLocalCoordID);

                    var globalCoordID = Helper.MapGrainIDToServiceID(myID, Constants.numGlobalCoord);
                    myGlobalCoord = myGrainFactory.GetGrain<IGlobalCoordGrain>(globalCoordID);
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
                siloID,
                myLocalCoordID,
                myLocalCoord,
                myGlobalCoord,
                myGrainFactory,
                myScheduler,
                state,
                log);

            nonDetTxnExecutor = new NonDetTxnExecutor<TState>(
                myID,
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
                myGrainFactory);

            return Task.CompletedTask;
        }

        /// <summary> Call this interface to submit a PACT to Snapper </summary>
        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput, List<int> grainAccessInfo, List<string> grainClassName)
        {
            var cxtInfo = await detTxnExecutor.GetDetContext(grainAccessInfo, grainClassName);
            var cxt = cxtInfo.Item2;
            if (highestCommittedBid < cxtInfo.Item1)
            {
                highestCommittedBid = cxtInfo.Item1;
                myScheduler.AckBatchCommit(highestCommittedBid);
            }

            // execute PACT
            var call = new FunctionCall(startFunc, funcInput, GetType());
            var resultObj = await ExecuteDet(call, cxt);

            // wait for this batch to commit
            await WaitForBatchCommit(cxt.localBid);

            var txnResult = new TransactionResult(resultObj);
            return txnResult;
        }

        /// <summary> Call this interface to submit an ACT to Snapper </summary>
        public async Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            var cxtInfo = await nonDetTxnExecutor.GetNonDetContext();
            if (highestCommittedBid < cxtInfo.Item1)
            {
                highestCommittedBid = cxtInfo.Item1;
                myScheduler.AckBatchCommit(highestCommittedBid);
            }
            var cxt = cxtInfo.Item2;

            // execute ACT
            var call = new FunctionCall(startFunc, funcInput, GetType());
            var funcResult = await ExecuteNonDet(call, cxt);

            // check serializability and do 2PC
            var canCommit = !funcResult.exception;
            var maxBeforeBid = -1;
            var res = new TransactionResult(funcResult.resultObj);
            var isPrepared = false;
            if (canCommit)
            {
                maxBeforeBid = funcResult.maxBeforeBid;
                var result = nonDetCommitter.CheckSerializability(highestCommittedBid, funcResult.maxBeforeBid, funcResult.minAfterBid, funcResult.isBeforeAfterConsecutive);
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

            if (canCommit) await nonDetCommitter.CoordCommit(cxt.globalTid, maxBeforeBid, funcResult.grainOpInfo);
            else
            {
                res.exception = true;
                await nonDetCommitter.CoordAbort(cxt.globalTid, funcResult.grainOpInfo, isPrepared);
            }

            // wait for previous batch to commit
            if (canCommit && highestCommittedBid < funcResult.maxBeforeBid)
            {
                var grainID = funcResult.grainWithHighestBeforeBid;
                if (grainID.Item1 == myID) await WaitForBatchCommit(funcResult.maxBeforeBid);
                else
                {
                    var grain = GrainFactory.GetGrain<ITransactionExecutionGrain>(grainID.Item1, grainID.Item2);
                    await grain.WaitForBatchCommit(funcResult.maxBeforeBid);
                }
            }

            return res;
        }

        /// <summary> Call this interface to emit a SubBatch from a local coordinator to a grain </summary>
        public Task ReceiveBatchSchedule(LocalSubBatch batch)
        {
            // do garbage collection for committed local batches
            if (highestCommittedBid < batch.highestCommittedBid)
            {
                highestCommittedBid = batch.highestCommittedBid;
                myScheduler.AckBatchCommit(highestCommittedBid);
            }
            batchCommit.Add(batch.bid, new TaskCompletionSource<bool>());

            // register the local SubBatch info
            myScheduler.RegisterBatch(batch, highestCommittedBid);
            detTxnExecutor.BatchArrive(batch);
            
            return Task.CompletedTask;
        }

        /// <summary> When commit an ACT, call this interface to wait for a specific local batch to commit </summary>
        public async Task WaitForBatchCommit(int bid)
        {
            if (highestCommittedBid >= bid) return;
            await batchCommit[bid].Task;
        }

        /// <summary> A local coordinator calls this interface to notify the commitment of a local batch </summary>
        public Task AckBatchCommit(int bid)
        {
            if (highestCommittedBid < bid)
            {
                highestCommittedBid = bid;
                myScheduler.AckBatchCommit(highestCommittedBid);
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

        public async Task<object> ExecuteDet(FunctionCall call, TransactionContext cxt)
        {
            await detTxnExecutor.WaitForTurn(cxt);
            var txnRes = await InvokeFunction(call, cxt);   // execute the function call;
            await detTxnExecutor.FinishExecuteDetTxn(cxt.localTid, cxt.localBid);
            detTxnExecutor.CleanUp(cxt.localTid);
            return txnRes.resultObj;
        }

        public async Task<NonDetFuncResult> ExecuteNonDet(FunctionCall call, TransactionContext cxt)
        {
            var canExecute = await nonDetTxnExecutor.WaitForTurn(cxt.globalTid);
            if (canExecute == false)
            {
                var funcResult = new NonDetFuncResult();
                funcResult.Exp_Deadlock = true;
                funcResult.exception = true;
                nonDetTxnExecutor.CleanUp(cxt.globalTid);
                return funcResult;
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
                var funcResult = nonDetTxnExecutor.UpdateExecutionResult(cxt.globalTid);
                if (resultObj != null) funcResult.SetResultObj(resultObj);
                nonDetTxnExecutor.CleanUp(cxt.globalTid);
                if (exception) CleanUp(cxt.globalTid);
                return funcResult;
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

        public async Task Commit(int tid, int maxBeforeBid)   // only writer grain needs 2nd phase of 2PC
        {
            nonDetTxnExecutor.Commit(maxBeforeBid);
            await nonDetCommitter.Commit(tid, maxBeforeBid);
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