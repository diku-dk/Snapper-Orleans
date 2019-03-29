using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Implementation;
using Concurrency.Interface;
using Microsoft.VisualStudio.TestTools.UnitTesting;


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
            foreach (var entry in keyValues) {
                var value = new int[entry.Value.Length];
                entry.Value.CopyTo(value,0);
                copiedValues.Add(entry.Key, value);
            }
            KeyValueState copy = new KeyValueState(copiedValues);
            return copy;
        }
    }

    [TestClass]
    class StateManagementTest
    {
        ITransactionalState  <KeyValueState> state;


        private KeyValueState createInitState()
        {
            //TODO: Seed the state
            return new KeyValueState();
        }
        [TestInitialize]
        public void Init()
        {
            if(state == null)
            {
                var seedState = createInitState();
                state = new HybridState<KeyValueState>(seedState);
            }
        }

        [TestMethod]
        public void SingleTransactionStateTransition()
        {
            //Test different modes R->RW->R-RW->RW->R->...
            Transaction 
            state.Read()
            Assert.Fail();
        }

        [TestMethod]
        public void InterLeavedExecutionStateTransition()
        {
            //Test concurrent reads

            //Test serializability of read write in presence of other read writes or reads

            //Test abort policy
        }
    }
}
