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
            TransactionContext ctx5 = new TransactionContext(0);
            var task5 = state.ReadWrite(ctx5);
            await state.Commit(ctx1.transactionID);
            Assert.IsFalse(task4.IsCompleted);
            await state.Abort(ctx2.transactionID);
            Assert.IsFalse(task5.IsCompleted);
            await state.Abort(ctx3.transactionID);
            await task4;
            Assert.IsTrue(task4.IsCompleted);            
            var state1 = await task4;
            state1.value += 100;            
            await state.Commit(ctx4.transactionID);
            var state2 = await task5;
            Assert.IsTrue(task5.IsCompleted);
            state2.value += 100;
            await state.Commit(ctx5.transactionID);
            var ctx7 = new TransactionContext(6);
            var task7 = state.Read(ctx7);
            Assert.IsTrue(task7.Result.value == 100200);

            //Test serializability of read write in presence of other read writes or reads

            //Test abort policy            
        }
    } 
}
