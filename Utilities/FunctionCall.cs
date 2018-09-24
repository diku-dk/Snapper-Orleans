using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Concurrency.Utilities
{
    [Serializable]
    public class FunctionCall
    {
        public TransactionContext context;
        public List<object> inputs;
        public int curPos;
        public TaskCompletionSource<List<object>> promise;
        public Type type;
        public string func;

        public FunctionCall(TransactionContext context, Type t, String func, List<object> inputs)
        {
            this.context = context;
            this.type = t;
            this.func = func;
            this.inputs = new List<object>(inputs);
            this.curPos = 0;
        }


    }
}
