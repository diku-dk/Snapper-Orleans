using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Implementation;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using AccountTransfer.Grains;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Utilities;


namespace Test.SystemTests
{
    [TestClass]
    public class Strict2PLStateManagementTest
    {
        ITransactionalState<Balance> state;

        private Balance createInitState()
        {
            //TODO: Seed the state
            return new Balance();
        }

        [TestInitialize]
        public void Init()
        {
            var seedState = createInitState();
            state = new HybridState<Balance>(seedState,ConcurrencyType.S2PL);
        }

        [TestMethod]
        public async Task InterLeavedExecutionStateTransition()
        {
            //Test concurrent reads            
            TransactionContext ctx1 = new TransactionContext(2);
            TransactionContext ctx2 = new TransactionContext(3);
            var task1 = state.Read(ctx1);
            var task2 = state.Read(ctx2);
            Assert.IsTrue(task1.IsCompleted && task2.IsCompleted);
            Assert.IsTrue(task1.Result.value == 100000);
            Assert.IsTrue(task2.Result.value == 100000);
            TransactionContext ctx3 = new TransactionContext(10);
            ctx3.isDeterministic = false;
            try
            {
                var task3 = state.ReadWrite(ctx3);
                await task3;
                Assert.Fail();
            } catch(DeadlockAvoidanceException)
            {
                ;
            }
            TransactionContext ctx4 = new TransactionContext(1);
            var task4 = state.ReadWrite(ctx4);
            
            Assert.IsFalse(task4.IsCompleted);
            Assert.IsTrue(task1.Result.value == task2.Result.value && task1.Result.value == 100000);
            state.Commit(ctx1.transactionID);
            Assert.IsFalse(task4.IsCompleted);
            state.Abort(ctx2.transactionID);
            await task4;
            Assert.IsTrue(task4.IsCompleted);
            task4.Result.value += 100;            
            state.Abort(ctx3.transactionID);
            state.Commit(ctx4.transactionID);


            //Test serializability of read write in presence of other read writes or reads

            //Test abort policy            
        }
    } 
}
