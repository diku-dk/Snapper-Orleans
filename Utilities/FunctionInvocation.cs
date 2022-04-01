using System;
using System.Collections.Generic;

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
    public class NonDetFuncResult : BasicFuncResult
    {
        public bool exception = false;
        public bool Exp_Deadlock = false;

        public int minAfterBid = -1;
        public int maxBeforeBid = -1;
        public bool isBeforeAfterConsecutive = true;

        public Tuple<int, string> grainWithHighestBeforeBid = new Tuple<int, string>(-1, "");

        public NonDetFuncResult() : base()
        {
        }

        public void MergeFuncResult(NonDetFuncResult res)
        {
            exception |= res.exception;
            Exp_Deadlock |= res.Exp_Deadlock;

            if (res.minAfterBid != -1 || res.minAfterBid < minAfterBid) 
                minAfterBid = res.minAfterBid;     // r.minAfterBid != -1

            if (maxBeforeBid < res.maxBeforeBid)   // r.maxBeforeBid != -1
            {
                maxBeforeBid = res.maxBeforeBid;
                grainWithHighestBeforeBid = res.grainWithHighestBeforeBid;
            }

            isBeforeAfterConsecutive &= res.isBeforeAfterConsecutive;

            MergeGrainOpInfo(res);
        }

        public void SetBeforeAfterInfo(int maxBeforeBid, int minAfterBid, bool isConsecutive, Tuple<int, string> grainFullID)
        {
            if (minAfterBid != -1 && minAfterBid < this.minAfterBid)
                this.minAfterBid = minAfterBid;

            if (this.maxBeforeBid < maxBeforeBid)   // maxBeforeBid != -1
            {
                this.maxBeforeBid = maxBeforeBid;
                grainWithHighestBeforeBid = grainFullID;
            }
            
            isBeforeAfterConsecutive &= isConsecutive;
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