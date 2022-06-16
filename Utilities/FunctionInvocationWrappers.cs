using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class TransactionResult
    {
        public object resultObject;

        public bool exception = false;
        public bool Exp_Deadlock = false;
        public bool Exp_Serializable = false;
        public bool Exp_NotSureSerializable = false;

        public DateTime beforeGetTidTime = DateTime.MaxValue;
        public DateTime afterGetTidTime = DateTime.MaxValue;
        public DateTime beforeExeTime = DateTime.MaxValue;
        public DateTime beforeUpdate1Time = DateTime.MaxValue;
        public DateTime callGrainTime = DateTime.MaxValue;
        public DateTime afterExeTime = DateTime.MaxValue;
        public DateTime beforeResolveTime = DateTime.MaxValue;
        public DateTime afterResolveTime = DateTime.MaxValue;

        public TransactionResult(object res = null)
        {
            resultObject = res;
        }

        public TransactionResult(bool exp, object res)
        {
            resultObject = res;
            exception = exp;
        }
    }

    [Serializable]
    public class FunctionResult
    {
        public bool exception;
        public int minAfterBid;
        public int maxBeforeBid;
        public bool Exp_Deadlock;
        public bool isNoOpOnGrain;
        public object resultObject;
        public bool isReadOnlyOnGrain;
        public bool isBeforeAfterConsecutive;
        public Tuple<int, string> grainWithHighestBeforeBid;
        public Dictionary<int, Tuple<string, bool>> grainsInNestedFunctions;   // <grainID, namespace, isReadonly>

        public DateTime beforeExeTime = DateTime.MaxValue;
        public DateTime beforeUpdate1Time = DateTime.MaxValue;
        public DateTime callGrainTime = DateTime.MaxValue;
        public DateTime afterExeTime = DateTime.MaxValue;

        public FunctionResult(object resultObject = null)
        {
            minAfterBid = -1;
            maxBeforeBid = -1;
            exception = false;
            Exp_Deadlock = false;
            isReadOnlyOnGrain = false;
            isBeforeAfterConsecutive = true;
            this.resultObject = resultObject;
            grainWithHighestBeforeBid = new Tuple<int, string>(-1, "");
            grainsInNestedFunctions = new Dictionary<int, Tuple<string, bool>>();
        }

        public void mergeWithFunctionResult(FunctionResult r)
        {
            Exp_Deadlock |= r.Exp_Deadlock;
            exception |= r.exception;
            foreach (var item in r.grainsInNestedFunctions)
            {
                if (grainsInNestedFunctions.ContainsKey(item.Key) == false)
                    grainsInNestedFunctions.Add(item.Key, item.Value);
                else
                {
                    var grainClassName = item.Value.Item1;
                    var isReadOnly = grainsInNestedFunctions[item.Key].Item2 && item.Value.Item2;
                    grainsInNestedFunctions[item.Key] = new Tuple<string, bool>(grainClassName, isReadOnly);
                }
            }

            if (maxBeforeBid < r.maxBeforeBid)   // r.maxBeforeBid != -1
            {
                maxBeforeBid = r.maxBeforeBid;
                grainWithHighestBeforeBid = r.grainWithHighestBeforeBid;
            }

            if (r.minAfterBid != -1 || r.minAfterBid < minAfterBid) minAfterBid = r.minAfterBid;   // r.minAfterBid != -1
            isBeforeAfterConsecutive &= r.isBeforeAfterConsecutive;
        }

        public void setSchedulingStatistics(int maxBeforeBid, int minAfterBid, bool consecutive, Tuple<int, string> id)
        {
            if (this.maxBeforeBid < maxBeforeBid)   // maxBeforeBid != -1
            {
                this.maxBeforeBid = maxBeforeBid;
                grainWithHighestBeforeBid = id;
            }
            if (minAfterBid != -1 && minAfterBid < this.minAfterBid) this.minAfterBid = minAfterBid;
            isBeforeAfterConsecutive &= consecutive;

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