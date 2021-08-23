using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class TransactionResult
    {
        public bool isDet = false;
        public object resultObject;

        public bool exception = false;
        public bool Exp_Deadlock = false;
        public bool Exp_Serializable = false;
        public bool Exp_NotSureSerializable = false;
        
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
        public object resultObject;
        public HashSet<int> afterSet;
        public HashSet<int> beforeSet;
        public bool isReadOnlyOnGrain;
        public bool isBeforeAfterConsecutive;
        public Tuple<int, string> grainWithHighestBeforeBid;
        public Dictionary<int, Tuple<string, bool>> grainsInNestedFunctions;   // <grainID, namespace, isReadonly>

        public FunctionResult(object resultObject = null)
        {
            exception = false;
            Exp_Deadlock = false;
            isReadOnlyOnGrain = false;
            minAfterBid = int.MaxValue;
            maxBeforeBid = int.MinValue;
            afterSet = new HashSet<int>();
            beforeSet = new HashSet<int>();
            isBeforeAfterConsecutive = false;
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

            if (beforeSet.Count == 0 && afterSet.Count == 0)
            {
                grainWithHighestBeforeBid = r.grainWithHighestBeforeBid;
                maxBeforeBid = r.maxBeforeBid;
                minAfterBid = r.minAfterBid;
                beforeSet = r.beforeSet;
                afterSet = r.afterSet;
            }
            else
            {
                beforeSet.UnionWith(r.beforeSet);
                afterSet.UnionWith(r.afterSet);
                if (maxBeforeBid < r.maxBeforeBid)
                {
                    maxBeforeBid = r.maxBeforeBid;
                    grainWithHighestBeforeBid = r.grainWithHighestBeforeBid;
                }
                minAfterBid = (minAfterBid > r.minAfterBid) ? r.minAfterBid : minAfterBid;
                isBeforeAfterConsecutive &= r.isBeforeAfterConsecutive;
            }
        }

        public void setSchedulingStatistics(int maxBeforeBid, int minAfterBid, bool consecutive, Tuple<int, string> id)
        {
            if (this.maxBeforeBid < maxBeforeBid)
            {
                this.maxBeforeBid = maxBeforeBid;
                grainWithHighestBeforeBid = id;
            }
            this.minAfterBid = (this.minAfterBid > minAfterBid) ? minAfterBid : this.minAfterBid;
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