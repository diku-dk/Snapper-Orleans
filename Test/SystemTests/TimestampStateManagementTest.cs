using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Implementation;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Utilities;


namespace Test.SystemTests
{
    [TestClass]
    public class TimestampStateManagementTest
    {
        ITransactionalState<KeyValueState> state;

        private KeyValueState createInitState()
        {
            //TODO: Seed the state
            return new KeyValueState();
        }

        //[TestInitialize]
        public void Init()
        {
            if (state == null)
            {
                var seedState = createInitState();
                state = new HybridState<KeyValueState>(seedState);
            }
        }

        [TestMethod]
        public async Task StateTransitionSingleTransaction()
        {
            state = new HybridState<KeyValueState>(createInitState());
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

        [TestMethod]
        public async Task InterLeavedExecutionStateTransition()
        {
            //Test concurrent reads
            state = new HybridState<KeyValueState>(createInitState());
            TransactionContext ctx1 = new TransactionContext(1, 2, Helper.convertUInt32ToGuid(0));
            ctx1.isDeterministic = false;
            TransactionContext ctx2 = new TransactionContext(1, 3, Helper.convertUInt32ToGuid(0));
            ctx2.isDeterministic = false;
            var task1 = state.Read(ctx1);
            var task2 = state.Read(ctx2);
            Assert.IsTrue(task1.IsCompleted && task2.IsCompleted);


            TransactionContext ctx3 = new TransactionContext(1, 10, Helper.convertUInt32ToGuid(0));
            ctx3.isDeterministic = false;
            var task3 = state.ReadWrite(ctx3);
            Assert.IsTrue(task3.IsCompleted);

            try
            {
                var task4 = state.ReadWrite(ctx3);
                await task4;
                Assert.Fail();
            }
            catch (DeadlockAvoidanceException)
            {
                ;
            }

            
            //Assert.IsFalse(task4.IsCompleted);
            //state.Commit(ctx1.transactionID);
            //Assert.IsFalse(task4.IsCompleted);
            //state.Abort(ctx2.transactionID);
            //await task4;
            //Assert.IsTrue(task4.IsCompleted);
            //state.Abort(ctx3.transactionID);
            //state.Commit(ctx4.transactionID);

            //Test serializability of read write in presence of other read writes or reads

            //Test abort policy            
        }
    }
}
