using System;
using Utilities;
using System.Collections.Generic;
using System.Threading.Tasks;
using Concurrency.Interface.TransactionExecution;
using Concurrency.Interface.Logging;
using Orleans;
using System.Runtime.Serialization;
using MessagePack;
using System.Diagnostics;

namespace Concurrency.Implementation.TransactionExecution.Nondeterministic
{
    // cannot support hybrid commit for Timestamp-based concurrency control
    public class NonDetCommitter<TState> where TState : ICloneable, ISerializable
    {
        readonly int myID;
        readonly Dictionary<int, int> coordinatorMap;    // <global ACT tid, the grain ID who starts the ACT>
        readonly ILoggingProtocol log;
        readonly IGrainFactory myGrainFactory;

        ITransactionalState<TState> state;

        public NonDetCommitter(
            int myID,
            Dictionary<int, int> coordinatorMap,
            ITransactionalState<TState> state, 
            ILoggingProtocol log, 
            IGrainFactory myGrainFactory)
        {
            this.myID = myID;
            this.state = state;
            this.log = log;
            this.myGrainFactory = myGrainFactory;
            this.coordinatorMap = coordinatorMap;
        }

        public void CheckGC()
        {
        }

        // serializable or not, sure or not sure
        public Tuple<bool, bool> CheckSerializability(
            NonDetScheduleInfo globalScheduleInfo,
            Dictionary<int, NonDetScheduleInfo> scheduleInfoPerSilo)
        {
            // check global bids
            var globalRes = Check(globalScheduleInfo.maxBeforeBid, globalScheduleInfo.minAfterBid, globalScheduleInfo.isAfterComplete);
            var isSerializable = globalRes.Item1;
            var isSure = globalRes.Item2;

            // check local bids in each silo
            foreach (var infoPerSilo in scheduleInfoPerSilo)
            {
                var info = infoPerSilo.Value;
                var res = Check(info.maxBeforeBid, info.minAfterBid, info.isAfterComplete);
                isSerializable &= res.Item1;
                isSure &= res.Item2;
            }

            return new Tuple<bool, bool>(isSerializable, isSure);
        }

        // serializable or not, sure or not sure
        Tuple<bool, bool> Check(int maxBeforeBid, int minAfterBid, bool isAfterComplete)
        {
            if (maxBeforeBid == -1) return new Tuple<bool, bool>(true, true);
            if (isAfterComplete && maxBeforeBid < minAfterBid) return new Tuple<bool, bool>(true, true);
            if (maxBeforeBid >= minAfterBid) return new Tuple<bool, bool>(false, true);
            return new Tuple<bool, bool>(false, false);
        }

        public async Task<bool> CoordPrepare(int tid, Dictionary<int, OpOnGrain> grainOpInfo)
        {
            if (log != null) await log.HandleBeforePrepareIn2PC(tid, myID, new HashSet<int>(grainOpInfo.Keys));

            var prepareTask = new List<Task<bool>>();
            foreach (var item in grainOpInfo)
            {
                if (item.Value.isNoOp) continue;  
                // reader grain needs to Prepare, because it should release the read lock
                // writer grain needs to Prepare, because it must persist the grain state
                var grain = myGrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, item.Value.grainClassName);
                prepareTask.Add(grain.Prepare(tid, item.Value.isReadonly));
            }
            await Task.WhenAll(prepareTask);

            foreach (var vote in prepareTask)
                if (vote.Result == false) return false;

            return true;
        }

        public async Task CoordCommit(int tid, NonDetFuncResult funcResult)
        {
            if (log != null) await log.HandleOnCommitIn2PC(tid, coordinatorMap[tid]);

            var commitTask = new List<Task>();
            foreach (var item in funcResult.grainOpInfo)
            {
                // if the grain has only been read or it's no-op, no need 2nd phase
                if (item.Value.isNoOp || item.Value.isReadonly) continue;   

                // writer grain needs 2nd phase, because it can only release the write lock in the 2nd phase
                var grain = myGrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, item.Value.grainClassName);
                var siloID = TransactionExecutionGrainPlacementHelper.MapGrainIDToSilo(item.Key);
                Debug.Assert(funcResult.scheduleInfoPerSilo.ContainsKey(siloID));
                commitTask.Add(grain.Commit(tid, funcResult.scheduleInfoPerSilo[siloID].maxBeforeBid, funcResult.globalScheduleInfo.maxBeforeBid));
            }
            await Task.WhenAll(commitTask);
        }

        // the ACT aborted due to RW conflicts will come to Abort phase directly (without Prepare phase)
        public async Task CoordAbort(int tid, Dictionary<int, OpOnGrain> grainOpInfo, bool isPrepared)
        {
            var abortTask = new List<Task>();
            // Presume Abort: we do not write abort logs, when recovering, if no log record is found, we assume the transaction was aborted
            foreach (var item in grainOpInfo)
            {
                // if the grain does no-op, no need 2nd phase
                if (item.Value.isNoOp) continue;
                if (isPrepared && item.Value.isReadonly) continue;
                // reader grain which has not been prepared needs to do Abort, because it needs to do garbage collection
                // writer grain needs 2nd phase, because it can only release the write lock in the 2nd phase
                var grain = myGrainFactory.GetGrain<ITransactionExecutionGrain>(item.Key, item.Value.grainClassName);
                abortTask.Add(grain.Abort(tid));
            }
            await Task.WhenAll(abortTask);
        }

        public async Task<bool> Prepare(int tid, bool isReader)
        {
            var vote = await state.Prepare(tid, isReader);
            if (vote && log != null && !isReader)
            {
                var data = MessagePackSerializer.Serialize(state.GetPreparedState(tid));
                await log.HandleOnPrepareIn2PC(data, tid, coordinatorMap[tid]);
            } 
            return vote;
        }

        public async Task Commit(int tid)
        {
            state.Commit(tid);
            if (log != null) await log.HandleOnCommitIn2PC(tid, coordinatorMap[tid]);
        }

        public void Abort(int tid)
        {
            state.Abort(tid);
        }
    }
}