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
    }

    [Serializable]
    public class FunctionResult
    {
        public int tid;
        public String txnType;
        public Dictionary<Guid, Object> beforeState;
        public Dictionary<Guid, Object> afterState;
        public Object resultObject;
        public Dictionary<Guid, String> grainsInNestedFunctions;
        public Boolean exception = false;
        public HashSet<int> beforeSet;
        public HashSet<int> afterSet;
        public int maxBeforeBid;
        public int minAfterBid;
        public Boolean isBeforeAfterConsecutive = false;
        public Boolean readOnly = true;
        public Tuple<Guid, String> grainWithHighestBeforeBid;
        public Boolean isDet = true;
        public Boolean Exp_RWConflict = false;
        public Boolean Exp_NotSerializable = false;
        public Boolean Exp_AppLogic = false;
        public Boolean Exp_2PC = false;
        public Boolean Exp_UnExpect = false;
        public int highestCommittedBid = -1;

        public FunctionResult(Object resultObject = null)
        {
            beforeState = new Dictionary<Guid, object>();
            afterState = new Dictionary<Guid, object>();
            this.resultObject = resultObject;
            grainsInNestedFunctions = new Dictionary<Guid, String>();
            beforeSet = new HashSet<int>();
            afterSet = new HashSet<int>();
            maxBeforeBid = int.MinValue;
            minAfterBid = int.MaxValue;
        }

        public void mergeWithFunctionResult(FunctionResult r)
        {
            foreach (var item in r.beforeState)
                // if the grain is accessed multiple times by a transaction
                if (!beforeState.ContainsKey(item.Key)) beforeState.Add(item.Key, item.Value);
            
            readOnly &= r.readOnly;
            if (this.highestCommittedBid < r.highestCommittedBid) this.highestCommittedBid = r.highestCommittedBid;
            this.Exp_AppLogic |= r.Exp_AppLogic;
            this.Exp_NotSerializable |= r.Exp_NotSerializable;
            this.Exp_RWConflict |= r.Exp_RWConflict;
            this.Exp_UnExpect |= r.Exp_UnExpect;

            this.exception |= r.exception;
            foreach (var entry in r.grainsInNestedFunctions)
                if (this.grainsInNestedFunctions.ContainsKey(entry.Key) == false)
                    this.grainsInNestedFunctions.Add(entry.Key, entry.Value);

            if (this.beforeSet.Count == 0 && this.afterSet.Count == 0)
            {
                this.grainWithHighestBeforeBid = r.grainWithHighestBeforeBid;
                this.maxBeforeBid = r.maxBeforeBid;
                this.minAfterBid = r.minAfterBid;
                this.beforeSet = r.beforeSet;
                this.afterSet = r.afterSet;
            }
            else
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

        public void setSchedulingStatistics(int maxBeforeBid, int minAfterBid, Boolean consecutive, Tuple<Guid, string> tuple)
        {

            if (this.maxBeforeBid < maxBeforeBid)
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
