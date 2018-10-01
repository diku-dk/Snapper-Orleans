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
        public Boolean exception = false;

        public FunctionResult(Object resultObject, FunctionResult r)
        {
            this.resultObject = resultObject;
            this.grainsInNestedFunctions = new HashSet<Guid>();
            this.grainsInNestedFunctions.UnionWith(r.grainsInNestedFunctions);
            this.exception = r.exception;
        }

        public FunctionResult(Object resultObject=null)
        {
            this.resultObject = resultObject;
            this.grainsInNestedFunctions = new HashSet<Guid>();
        }

        public FunctionResult(FunctionResult r1, FunctionResult r2)
        {
            exception = r1.exception | r2.exception;
            grainsInNestedFunctions = new HashSet<Guid>();
            grainsInNestedFunctions.UnionWith(r1.grainsInNestedFunctions);
            grainsInNestedFunctions.UnionWith(r2.grainsInNestedFunctions);  

        }

        public void mergeWithFunctionResult(FunctionResult r)
        {
            this.exception |= r.exception;
            this.grainsInNestedFunctions.UnionWith(r.grainsInNestedFunctions);
        }

        public void setResult(Object result)
        {
            resultObject = result;
        }

        public void setException(Boolean b)
        {
            exception = b;
        }

        public Boolean hasException()
        {
            return exception;
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
