﻿using Concurrency.Interface.Coordinator;
using Concurrency.Interface.TransactionExecution;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution
{
    public class NonDetTxnExecutor<TState> where TState : ICloneable
    {
        readonly int myID;
        readonly string myClassName;
        readonly Tuple<int, string> myFullID;
        readonly ILocalCoordGrain myLocalCoord;
        readonly IGlobalCoordGrain myGlobalCoord;

        ITransactionalState<TState> state;
        TransactionScheduler myScheduler;

        // ACT execution
        Dictionary<int, NonDetFuncResult> nonDetFuncResults;                   // key: global ACT tid

        // hybrid execution
        // TODO: in multi-silo deployment, we need to check both local batch and global batch!!!!!!!!!!!!
        int maxBeforeBidOnGrain;                                               // maxBeforeBid of the current executing / latest executed ACT
        readonly TimeSpan deadlockTimeout;                                     // detect deadlock between ACT and batches

        public void CheckGC()
        {
            if (nonDetFuncResults.Count != 0) Console.WriteLine($"NonDetTxnExecutor: nonDetFuncResults.Count = {nonDetFuncResults.Count}");
        }

        public NonDetTxnExecutor(
            int myID,
            string myClassName,
            ILocalCoordGrain myLocalCoord,
            IGlobalCoordGrain myGlobalCoord,
            TransactionScheduler myScheduler, 
            ITransactionalState<TState> state)
        {
            this.myID = myID;
            this.myClassName = myClassName;
            this.myLocalCoord = myLocalCoord;
            this.myGlobalCoord = myGlobalCoord;
            this.myScheduler = myScheduler;
            this.state = state;

            myFullID = new Tuple<int, string>(myID, myClassName);
            nonDetFuncResults = new Dictionary<int, NonDetFuncResult>();
            maxBeforeBidOnGrain = -1;
            deadlockTimeout = TimeSpan.FromMilliseconds(20);
        }

        public async Task<Tuple<int, TransactionContext>> GetNonDetContext()
        {
            var highestCommittedBid = -1;
            TransactionRegistInfo info;
            if (Constants.multiSilo && Constants.hierarchicalCoord)
                info = await myGlobalCoord.NewTransaction();
            else
            {
                info = await myLocalCoord.NewTransaction();
                highestCommittedBid = info.highestCommittedBid;
            }
            var cxt = new TransactionContext(info.tid, myID, false);
            return new Tuple<int, TransactionContext>(highestCommittedBid, cxt);
        }

        public async Task<bool> WaitForTurn(int tid)
        {
            // wait for turn to execute
            var t = myScheduler.WaitForTurn(tid);
            await Task.WhenAny(t, Task.Delay(deadlockTimeout));
            if (t.IsCompleted)
            {
                Debug.Assert(nonDetFuncResults.ContainsKey(tid) == false);
                nonDetFuncResults.Add(tid, new NonDetFuncResult());
            }
            else myScheduler.scheduleInfo.CompleteNonDetTxn(tid);
            return t.IsCompleted;
        }

        public async Task<TState> GetState(int tid, AccessMode mode)
        {
            try
            {
                if (mode == AccessMode.Read)
                {
                    var myState = await state.NonDetRead(tid);
                    nonDetFuncResults[tid].isNoOpOnGrain = false;
                    nonDetFuncResults[tid].isReadOnlyOnGrain = true;
                    return myState;
                }
                else
                {
                    var myState = await state.NonDetReadWrite(tid);
                    nonDetFuncResults[tid].isNoOpOnGrain = false;
                    nonDetFuncResults[tid].isReadOnlyOnGrain = false;
                    return myState;
                }
            }
            catch (Exception)   // DeadlockAvoidanceException
            {
                nonDetFuncResults[tid].exception = true;
                Debug.Assert(nonDetFuncResults[tid].isNoOpOnGrain && nonDetFuncResults[tid].isReadOnlyOnGrain);
                throw;
            }
        }

        public async Task<TransactionResult> CallGrain(TransactionContext cxt, FunctionCall call, ITransactionExecutionGrain grain)
        {
            var funcResult = await grain.ExecuteNonDet(call, cxt);
            nonDetFuncResults[cxt.globalTid].MergeFuncResult(funcResult);
            return new TransactionResult(funcResult.resultObj);
        }

        // Update the metadata of the execution results, including accessed grains, before/after set, etc.
        public NonDetFuncResult UpdateExecutionResult(int tid)
        {
            var res = nonDetFuncResults[tid];

            if (res.grainWithHighestBeforeBid.Item1 == -1) res.grainWithHighestBeforeBid = myFullID;

            if (res.grainOpInfo.ContainsKey(myID) == false)
                res.grainOpInfo.Add(myID, new OpOnGrain(myClassName, res.isNoOpOnGrain, res.isReadOnlyOnGrain));

            var result = myScheduler.scheduleInfo.GetBeforeAfter(tid);   // <maxBeforeBid, minAfterBid, isConsecutive>
            var maxBeforeBid = result.Item1;
            if (maxBeforeBidOnGrain > maxBeforeBid) maxBeforeBid = maxBeforeBidOnGrain;
            res.SetBeforeAfterInfo(maxBeforeBid, result.Item2, result.Item3, myFullID);

            return res;
        }

        public void Commit(int maxBeforeBid)
        {
            Debug.Assert(maxBeforeBidOnGrain <= maxBeforeBid);
            maxBeforeBidOnGrain = maxBeforeBid;
        }

        public void CleanUp(int tid)
        {
            nonDetFuncResults.Remove(tid);
        }
    }
}