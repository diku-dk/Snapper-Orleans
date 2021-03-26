using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class FunctionInput
    {
        public object inputObject;
        public TransactionContext context;

        public FunctionInput(FunctionInput input, object data = null)
        {
            context = input.context;
            inputObject = data;
        }

        public FunctionInput(object data = null)
        {
            inputObject = data;
        }
    }

    [Serializable]
    public class TransactionResult
    {
        public bool exception;
        public object resultObject;
        public bool Exp_Serializable;
        public bool Exp_NotSureSerializable;
        public bool Exp_Deadlock;
        public bool isDet = false;

        public TransactionResult(object res = null)
        {
            resultObject = res;
            exception = false;
            Exp_Serializable = false;
            Exp_NotSureSerializable = false;
            Exp_Deadlock = false;
        }

        public TransactionResult(bool exp, object res)
        {
            resultObject = res;
            exception = exp;
            Exp_Serializable = false;
            Exp_NotSureSerializable = false;
            Exp_Deadlock = false;
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
        public Dictionary<Tuple<int, string>, bool> grainsInNestedFunctions;   // <namespace, grainID, isReadonly>

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
            grainsInNestedFunctions = new Dictionary<Tuple<int, string>, bool>();
        }

        public void mergeWithFunctionResult(FunctionResult r)
        {
            Exp_Deadlock |= r.Exp_Deadlock;
            exception |= r.exception;
            foreach (var item in r.grainsInNestedFunctions)
            {
                if (grainsInNestedFunctions.ContainsKey(item.Key) == false)
                    grainsInNestedFunctions.Add(item.Key, item.Value);
                else grainsInNestedFunctions[item.Key] |= item.Value;
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

        public void setResult(object result)
        {
            resultObject = result;
        }

        public void setException()
        {
            exception = true;
        }

        public void expandBeforeandAfterSet(HashSet<int> bSet, HashSet<int> aSet)
        {
            beforeSet.UnionWith(bSet);
            afterSet.UnionWith(aSet);
        }
    }

    [Serializable]
    public class FunctionCall
    {
        public FunctionInput funcInput;
        public Type type;
        public string func;

        public FunctionCall(Type t, string func, FunctionInput funcInput)
        {
            type = t;
            this.func = func;
            this.funcInput = funcInput;
        }
    }
}
