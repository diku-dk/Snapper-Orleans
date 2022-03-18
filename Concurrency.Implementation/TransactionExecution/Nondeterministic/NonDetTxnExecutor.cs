using Concurrency.Implementation.TransactionExecution.Nondeterministic;
using Concurrency.Interface.Coordinator;
using Concurrency.Interface.TransactionExecution;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation.TransactionExecution
{
    public class NonDetTxnExecutor<TState>
    {
        readonly int myID;
        readonly string myClassName;
        readonly Tuple<int, string> myFullID;
        readonly ILocalCoordGrain myLocalCoord;
        readonly IGlobalCoordGrain myGlobalCoord;

        ITransactionalState<TState> state;
        TransactionScheduler myScheduler;

        // ACT execution
        Dictionary<int, int> coordinatorMap;
        Dictionary<int, NonDetFuncResult> nonDetFuncResults;                   // key: global ACT tid

        // hybrid execution
        // TODO: in multi-silo deployment, we need to check both local batch and global batch!!!!!!!!!!!!
        int maxBeforeBidOnGrain;                                             // maxBeforeBid of the current executing / latest executed ACT
        TimeSpan deadlockTimeout;                                            // detect deadlock between ACT and batches

        public NonDetTxnExecutor(
            int myID,
            string myClassName,
            ILocalCoordGrain myLocalCoord,
            IGlobalCoordGrain myGlobalCoord,
            TransactionScheduler myScheduler, 
            Dictionary<int, int> coordinatorMap,
            ITransactionalState<TState> state)
        {
            this.myID = myID;
            this.myClassName = myClassName;
            this.myLocalCoord = myLocalCoord;
            this.myGlobalCoord = myGlobalCoord;
            this.myScheduler = myScheduler;
            this.coordinatorMap = coordinatorMap;
            this.state = state;

            myFullID = new Tuple<int, string>(myID, myClassName);
            nonDetFuncResults = new Dictionary<int, NonDetFuncResult>();
            maxBeforeBidOnGrain = -1;
            deadlockTimeout = TimeSpan.FromMilliseconds(20);
        }

        public void CheckGC()
        { 
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

        public async Task<NonDetFuncResult> ExecuteNonDet(FunctionCall call, TransactionContext cxt)
        {
            Debug.Assert(nonDetFuncResults.ContainsKey(cxt.globalTid) == false);
            nonDetFuncResults.Add(cxt.globalTid, new NonDetFuncResult());
            var funcResult = nonDetFuncResults[cxt.globalTid];

            // wait for turn to execute
            var t = myScheduler.waitForTurn(cxt.globalTid);
            await Task.WhenAny(t, Task.Delay(deadlockTimeout));
            if (t.IsCompleted)
            {
                var txnRes = await InvokeFunction(call, cxt);
                funcResult.exception |= txnRes.exception;
                funcResult.SetResultObj(txnRes.resultObj);
                updateExecutionResult(cxt.globalTid, funcResult);
            }
            else   // time out!!!
            {
                funcResult.Exp_Deadlock = true;
                funcResult.exception = true;
                updateExecutionResult(funcResult);
            }

            return funcResult;
        }

        public Task<TState> GetState(int tid, AccessMode mode)
        {
            if (mode == AccessMode.Read)
            {
                nonDetFuncResults[tid].isNoOpOnGrain = false;
                nonDetFuncResults[tid].isReadOnlyOnGrain = true;
                return state.nonDetRead(tid);
            }
            else
            {
                nonDetFuncResults[tid].isNoOpOnGrain = false;
                nonDetFuncResults[tid].isReadOnlyOnGrain = false;
                return state.nonDetReadWrite(tid);
            }
        }

        public async Task<TransactionResult> CallGrain(TransactionContext cxt, FunctionCall call, ITransactionExecutionGrain grain)
        {
            var funcResult = await grain.ExecuteNonDet(call, cxt);
            nonDetFuncResults[cxt.globalTid].mergeFuncResult(funcResult);
            return new TransactionResult(funcResult.resultObj);
        }

        void updateExecutionResult(NonDetFuncResult res)
        {
            if (res.grainOpInfo.ContainsKey(myID) == false)
                res.grainOpInfo.Add(myID, new OpOnGrain(myClassName, true, true));
        }

        // Update the metadata of the execution results, including accessed grains, before/after set, etc.
        void updateExecutionResult(int tid, NonDetFuncResult res)
        {
            if (res.grainWithHighestBeforeBid.Item1 == -1) res.grainWithHighestBeforeBid = myFullID;

            if (res.grainOpInfo.ContainsKey(myID) == false)
                res.grainOpInfo.Add(myID, new OpOnGrain(myClassName, res.isNoOpOnGrain, res.isReadOnlyOnGrain));

            var result = myScheduler.scheduleInfo.getBeforeAfter(tid);   // <maxBeforeBid, minAfterBid, isConsecutive>
            var maxBeforeBid = result.Item1;
            if (maxBeforeBidOnGrain > maxBeforeBid) maxBeforeBid = maxBeforeBidOnGrain;
            res.setBeforeAfterInfo(maxBeforeBid, result.Item2, result.Item3, myFullID);
        }

        async Task<TransactionResult> InvokeFunction(FunctionCall call, TransactionContext cxt)
        {
            try
            {
                if (cxt.localBid == -1) coordinatorMap.Add(cxt.globalTid, cxt.nonDetCoordID);
                var mi = call.grainClassName.GetMethod(call.funcName);
                var t = (Task<TransactionResult>)mi.Invoke(this, new object[] { cxt, call.funcInput });
                return await t;
            }
            catch (Exception e)
            {
                Console.WriteLine($"{e.Message} {e.StackTrace}");
                throw e;
            }
        }

        public void Commit(int maxBeforeBid)
        {
            Debug.Assert(maxBeforeBidOnGrain <= maxBeforeBid);
            maxBeforeBidOnGrain = maxBeforeBid;
        }
    }
}