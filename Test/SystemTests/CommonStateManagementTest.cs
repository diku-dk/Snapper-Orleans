using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using Concurrency.Implementation;
using Utilities;


namespace Test.SystemTests
{
    public class KeyValueState : ICloneable
    {
        Dictionary<int, int[]> keyValues;

        public KeyValueState()
        {
            keyValues = new Dictionary<int, int[]>();
        }

        public KeyValueState(Dictionary<int, int[]> keyValues)
        {
            this.keyValues = keyValues;
        }

        object ICloneable.Clone()
        {
            var copiedValues = new Dictionary<int, int[]>();
            foreach (var entry in keyValues)
            {
                var value = new int[entry.Value.Length];
                entry.Value.CopyTo(value, 0);
                copiedValues.Add(entry.Key, value);
            }
            KeyValueState copy = new KeyValueState(copiedValues);
            return copy;
        }
    }

    [TestClass]
    public class CommonStateManagementTest
    {
        HashSet<ConcurrencyType> concurrencyControlMethodsToTest;       

        [TestInitialize]
        public void Init()
        {
            if(concurrencyControlMethodsToTest == null)
            {
                concurrencyControlMethodsToTest = new HashSet<ConcurrencyType>();
                concurrencyControlMethodsToTest.Add(ConcurrencyType.S2PL);
                concurrencyControlMethodsToTest.Add(ConcurrencyType.TIMESTAMP);
            }
        }
        private ITransactionalState<KeyValueState> createState(ConcurrencyType type)
        {
            //TODO: Seed the state
            return new HybridState<KeyValueState>(new KeyValueState(), type);
        }        

        [TestMethod]
        public async Task StateTransitionSingleTransaction()            
        {
            foreach(var aType in concurrencyControlMethodsToTest)
            {
                var state = createState(aType);
                //Test different modes R->RW->R-RW->RW->R->...
                TransactionContext ctx = new TransactionContext(1, 1, Helper.convertUInt32ToGuid(0));
                ctx.isDeterministic = false;
                var task = state.Read(ctx);
                Assert.IsTrue(task.IsCompleted);
                task = state.Read(ctx);
                Assert.IsTrue(task.IsCompleted);
                task = state.ReadWrite(ctx);
                Assert.IsTrue(task.IsCompleted);
                task = state.ReadWrite(ctx);
                Assert.IsTrue(task.IsCompleted);
                await state.Abort(ctx.transactionID);
            }
        }
    }
}
