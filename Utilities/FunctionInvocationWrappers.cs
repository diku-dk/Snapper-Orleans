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

        public FunctionInput(FunctionInput input, List<object> data)
        {
            context = input.context;
            inputObjects = data;
        }
        public FunctionInput(List<object> data)
        {
            inputObjects = data;
        }


    }

    [Serializable]
    public class FunctionResult
    {
        public Object resultObject;
        public ISet<Guid> grainsInNestedFunctions;

        public FunctionResult(Object resultObject, FunctionResult ret)
        {
            this.resultObject = resultObject;
            this.grainsInNestedFunctions = ret.grainsInNestedFunctions;
        }

        public FunctionResult(Object resultObject=null)
        {
            this.resultObject = resultObject;
            this.grainsInNestedFunctions = new HashSet<Guid>();
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
