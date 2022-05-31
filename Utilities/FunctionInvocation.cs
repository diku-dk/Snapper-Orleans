using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Utilities
{
    [Serializable]
    public class TransactionResult
    {
        public object resultObj;

        // only for ACT
        public bool exception = false;
        public bool Exp_Deadlock = false;
        public bool Exp_Serializable = false;
        public bool Exp_NotSureSerializable = false;

        // investigate PACT breakdown latency
        public double prepareTime;    // receive txn request ==> start execute txn
        public double executeTime;    // start execute txn   ==> finish execute txn
        public double commitTime;     // finish execute txn  ==> batch has committed

        public TransactionResult(object resultObj = null)
        {
            this.resultObj = resultObj;
        }
    }

    [Serializable]
    public class OpOnGrain
    {
        public bool isNoOp;
        public bool isReadonly;
        public readonly string grainClassName;

        public OpOnGrain(string grainClassName, bool isNoOp, bool isReadonly)
        {
            this.grainClassName = grainClassName;
            this.isNoOp = isNoOp;
            this.isReadonly = isReadonly;
        }
    }

    [Serializable]
    public class BasicFuncResult
    {
        public object resultObj;
        public bool isNoOpOnGrain = true;
        public bool isReadOnlyOnGrain = true;
        public Dictionary<int, OpOnGrain> grainOpInfo = new Dictionary<int, OpOnGrain>();   // <grainID, operations performed on the grains>

        public void SetResultObj(object resultObj)
        {
            this.resultObj = resultObj;
        }

        public void MergeGrainOpInfo(BasicFuncResult res)
        {
            foreach (var item in res.grainOpInfo)
            {
                if (grainOpInfo.ContainsKey(item.Key) == false)
                    grainOpInfo.Add(item.Key, item.Value);
                else
                {
                    var grainClassName = item.Value.grainClassName;
                    var isReadOnly = grainOpInfo[item.Key].isReadonly && item.Value.isReadonly;
                    var isNoOp = grainOpInfo[item.Key].isNoOp && item.Value.isNoOp;
                    grainOpInfo[item.Key] = new OpOnGrain(grainClassName, isNoOp, isReadOnly);
                }
            }
        }
    }

    [Serializable]
    public class NonDetScheduleInfo
    {
        public int maxBeforeBid;
        public int minAfterBid;
        public bool isAfterComplete;

        public NonDetScheduleInfo()
        {
            maxBeforeBid = -1;
            minAfterBid = int.MaxValue;
            isAfterComplete = true;
        }

        public NonDetScheduleInfo(int maxBeforeBid, int minAfterBid, bool isAfterComplete)
        {
            this.maxBeforeBid = maxBeforeBid;
            this.minAfterBid = minAfterBid;
            this.isAfterComplete = isAfterComplete;
        }
    }

    [Serializable]
    public class NonDetFuncResult : BasicFuncResult
    {
        public bool exception = false;
        public bool Exp_Deadlock = false;

        // this info is used to check global serializability
        public NonDetScheduleInfo globalScheduleInfo;
        public Dictionary<int, NonDetScheduleInfo> scheduleInfoPerSilo;     // <silo ID, scheule info>

        // <silo ID, grain ID, grain name>
        // this info is used to commit an ACT after its dependent batch has committed
        public Dictionary<int, Tuple<int, string>> grainWithMaxBeforeLocalBidPerSilo;

        public NonDetFuncResult() : base()
        {
            globalScheduleInfo = new NonDetScheduleInfo();
            scheduleInfoPerSilo = new Dictionary<int, NonDetScheduleInfo>();
            grainWithMaxBeforeLocalBidPerSilo = new Dictionary<int, Tuple<int, string>>();
        }

        public void MergeFuncResult(NonDetFuncResult res)
        {
            exception |= res.exception;
            Exp_Deadlock |= res.Exp_Deadlock;

            foreach (var info in res.scheduleInfoPerSilo)
                MergeBeforeAfterLocalInfo(info.Value, res.grainWithMaxBeforeLocalBidPerSilo[info.Key], info.Key);

            MergeBeforeAfterGlobalInfo(res.globalScheduleInfo);

            MergeGrainOpInfo(res);
        }

        public void MergeBeforeAfterLocalInfo(
            NonDetScheduleInfo newLocalInfo, 
            Tuple<int, string> grainFullID, 
            int mySiloID)
        {
            if (scheduleInfoPerSilo.ContainsKey(mySiloID) == false)
            {
                scheduleInfoPerSilo.Add(mySiloID, newLocalInfo);
                grainWithMaxBeforeLocalBidPerSilo.Add(mySiloID, grainFullID);
            }
            else
            {
                var myLocalInfo = scheduleInfoPerSilo[mySiloID];
                
                if (myLocalInfo.maxBeforeBid < newLocalInfo.maxBeforeBid)
                {
                    myLocalInfo.maxBeforeBid = newLocalInfo.maxBeforeBid;
                    Debug.Assert(grainWithMaxBeforeLocalBidPerSilo.ContainsKey(mySiloID));
                    grainWithMaxBeforeLocalBidPerSilo[mySiloID] = grainFullID;
                }

                myLocalInfo.minAfterBid = Math.Min(myLocalInfo.minAfterBid, newLocalInfo.minAfterBid);
                myLocalInfo.isAfterComplete &= newLocalInfo.isAfterComplete;
            }
        }

        public void MergeBeforeAfterGlobalInfo(NonDetScheduleInfo newGlobalInfo)
        {
            globalScheduleInfo.maxBeforeBid = Math.Max(globalScheduleInfo.maxBeforeBid, newGlobalInfo.maxBeforeBid);
            globalScheduleInfo.minAfterBid = Math.Min(globalScheduleInfo.minAfterBid, newGlobalInfo.minAfterBid);
            globalScheduleInfo.isAfterComplete &= newGlobalInfo.isAfterComplete;
        }
    }

    [Serializable]
    public class FunctionCall
    {
        public readonly string funcName;
        public readonly object funcInput;
        public readonly Type grainClassName;

        public FunctionCall(string funcName, object funcInput, Type grainClassName)
        {
            this.funcName = funcName;
            this.funcInput = funcInput;
            this.grainClassName = grainClassName;
        }
    }
}