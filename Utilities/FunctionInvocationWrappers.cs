using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Utilities
{
    [Serializable]
    public class FunctionInput
    {
        public Object inputObject;
        public TransactionContext context;

        public FunctionInput(FunctionInput input, Object data)
        {
            context = input.context;
            inputObject = data;
        }
        public FunctionInput(Object data)
        {
            inputObject = data;
        }

        public FunctionInput()
        {
        }
    }

    [Serializable]
    public class FunctionResult
    {
        public Object resultObject;
        public Dictionary<Guid, String> grainsInNestedFunctions;
        public Boolean exception = false;
        public HashSet<int> beforeSet;
        public HashSet<int> afterSet;
        public int maxBeforeBid;
        public int minAfterBid;
        public Boolean isBeforeAfterConsecutive = false;
        public Boolean readOnly = false;
        public Tuple<Guid, String> grainWithHighestBeforeBid; 

        public FunctionResult(Object resultObject, FunctionResult r)
        {
            this.resultObject = resultObject;
            this.grainsInNestedFunctions = new Dictionary<Guid, String>();
            foreach (var entry in r.grainsInNestedFunctions)
                this.grainsInNestedFunctions.Add(entry.Key, entry.Value);
            this.exception = r.exception;
            this.isBeforeAfterConsecutive = r.isBeforeAfterConsecutive;
            this.beforeSet = new HashSet<int>();
            this.afterSet = new HashSet<int>();
        }

        public FunctionResult(Object resultObject=null)
        {
            this.resultObject = resultObject;
            this.grainsInNestedFunctions = new Dictionary<Guid, String>();
            this.beforeSet = new HashSet<int>();
            this.afterSet = new HashSet<int>();
            this.maxBeforeBid = int.MinValue;
            this.minAfterBid = int.MaxValue;
        }

        public void mergeWithFunctionResult(FunctionResult r)
        {
            this.exception |= r.exception;
            foreach (var entry in r.grainsInNestedFunctions)
                if(this.grainsInNestedFunctions.ContainsKey(entry.Key) == false)
                    this.grainsInNestedFunctions.Add(entry.Key,entry.Value);

            if(this.beforeSet.Count == 0 && this.afterSet.Count == 0)
            {
                this.grainWithHighestBeforeBid = r.grainWithHighestBeforeBid;
                this.maxBeforeBid = r.maxBeforeBid;
                this.minAfterBid = r.minAfterBid;
                this.beforeSet = r.beforeSet;
                this.afterSet = r.afterSet;
            } else
            {
                this.beforeSet.UnionWith(r.beforeSet);
                this.afterSet.UnionWith(r.afterSet);
                if (this.maxBeforeBid < r.maxBeforeBid)
                {
                    this.maxBeforeBid = r.maxBeforeBid;
                    grainWithHighestBeforeBid = r.grainWithHighestBeforeBid;
                }
                this.minAfterBid = (this.minAfterBid > r.minAfterBid) ? r.minAfterBid : this.minAfterBid;
                isBeforeAfterConsecutive &= r.isBeforeAfterConsecutive;
            }            
        }

        public void setSchedulingStatistics(int maxBeforeBid, int minAfterBid, Boolean consecutive, Tuple<Guid, string>  tuple)
        {

            if(this.maxBeforeBid < maxBeforeBid)
            {
                this.maxBeforeBid = maxBeforeBid;
                grainWithHighestBeforeBid = tuple;
            }
            minAfterBid = (this.minAfterBid > minAfterBid) ? minAfterBid : this.minAfterBid;
            isBeforeAfterConsecutive &= consecutive;
        }

        public void setResult(Object result)
        {
            resultObject = result;
        }

        public void setException()
        {
            exception = true;
        }

        public Boolean hasException()
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

        public FunctionCall(Type t, String func, FunctionInput funcInput)
        {        
            this.type = t;
            this.func = func;
            this.funcInput = funcInput;    
        }
    }

    

    
}
