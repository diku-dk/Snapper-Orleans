using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class FunctionInput
    {
        public object inputObject;
        public TransactionContext context;

        public FunctionInput(FunctionInput input, object data)
        {
            context = input.context;
            inputObject = data;
        }

        public FunctionInput(object data)
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
        public bool Exp_Deadlock;
        public bool isDet = true;

        public TransactionResult(object res = null)
        {
            resultObject = res;
            exception = false;
            Exp_Serializable = false;
            Exp_Deadlock = false;
        }

        public TransactionResult(bool exp, object res)
        {
            resultObject = res;
            exception = exp;
            Exp_Serializable = false;
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
        public bool isBeforeAfterConsecutive;
        public int grainWithHighestBeforeBid;
        public HashSet<int> grainsInNestedFunctions;
        
        public FunctionResult(object resultObject = null)
        {
            exception = false;
            Exp_Deadlock = false;
            minAfterBid = int.MaxValue;
            maxBeforeBid = int.MinValue;
            afterSet = new HashSet<int>();
            beforeSet = new HashSet<int>();
            grainWithHighestBeforeBid = -1;
            isBeforeAfterConsecutive = false;
            this.resultObject = resultObject;
            grainsInNestedFunctions = new HashSet<int>();
        }

        public void mergeWithFunctionResult(FunctionResult r)
        {
            Exp_Deadlock |= r.Exp_Deadlock;
            exception |= r.exception;
            foreach (var grain in r.grainsInNestedFunctions)
                if (grainsInNestedFunctions.Contains(grain) == false) grainsInNestedFunctions.Add(grain);

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

        public void setSchedulingStatistics(int maxBeforeBid, int minAfterBid, bool consecutive, int coordID)
        {
            if (this.maxBeforeBid < maxBeforeBid)
            {
                this.maxBeforeBid = maxBeforeBid;
                grainWithHighestBeforeBid = coordID;
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

        public bool hasException()
        {
            return (exception == true);
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
