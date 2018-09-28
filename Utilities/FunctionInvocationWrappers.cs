using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Utilities
{
    [Serializable]
    public class FunctionInput
    {
        public List<object> inputObjects;
        public TransactionContext context;
    }

    [Serializable]
    public class FunctionResult
    {
        public Object resultObject;
        public ISet<Guid> grainsInNestedFunctions;

        public FunctionResult(Object resultObject)
        {
            this.resultObject = resultObject;
            this.grainsInNestedFunctions = new HashSet<Guid>();
        }
    }

    [Serializable]
    public class FunctionCall
    {
        //public TransactionContext context;
        public FunctionInput funcInput;
        //public int curPos;
        //public TaskCompletionSource<List<object>> promise;
        public Type type;
        public string func;

        public FunctionCall(TransactionContext context, Type t, String func, FunctionInput funcInput)
        {
            //this.context = context;
            this.type = t;
            this.func = func;
            this.funcInput = funcInput;
            //this.curPos = 0;
        }
    }

    

    
}
