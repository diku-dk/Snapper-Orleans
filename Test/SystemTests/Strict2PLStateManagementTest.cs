using System;
using System.Collections.Generic;
using System.Collections;
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
        Random rnd = new Random();

        private Balance createInitState()
        {
            //TODO: Seed the state
            return new Balance();
        }

        [TestInitialize]
        public void Init()
        {
            var seedState = createInitState();
            state = new HybridState<Balance>(seedState, ConcurrencyType.S2PL);
        }

        private HashSet<int> GenerateRandomIds(int min, int max, int num)
        {
            var ids = new HashSet<int>();
            if(num > (max - min))
            {
                throw new Exception("Cannot generate more unique numbers than range of values");
            }
            while(ids.Count != num)
            {
                //Not very efficient, change to reservoir sampling
                var val = rnd.Next(min,max);
                ids.Add(val);
            }
            return ids;
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
            }
            catch (DeadlockAvoidanceException)
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


        [TestMethod]
        public async Task InterleavedWriters()
        {
            int numConcurrentWrites = 10000;
            int numAborts = 1000;            
            var idsToAbort = GenerateRandomIds(1, numConcurrentWrites, numAborts);
            var tasks = new Task<Balance>[numConcurrentWrites];
            for (int i = numConcurrentWrites; i > 0; i--)
            {
                var ctx = new TransactionContext(i);
                var task = state.ReadWrite(ctx);
                tasks[i - 1] = task;
                if (i == numConcurrentWrites)
                {
                    Assert.IsTrue(task.IsCompleted);
                }
                else
                {
                    Assert.IsFalse(task.IsCompleted);
                }
            }

            for (int i = numConcurrentWrites - 1; i >= 0; i--)
            {
                var task = tasks[i];
                var myState = await task;
                myState.value += 1;
                if (idsToAbort.Contains(i))
                {
                    await state.Abort(i+1);
                }
                else
                {
                    await state.Commit(i+1);
                }
            }

            var ctx0 = new TransactionContext(0);
            var task0 = state.Read(ctx0);
            Assert.IsTrue(task0.Result.value == (100000 + numConcurrentWrites - numAborts));
        }

        [TestMethod]
        public async Task InterleavedReaders()
        {
            int numConcurrentReads = 10000;
            int numAborts = 1000;
            var idsToAbort = GenerateRandomIds(1, numConcurrentReads, numAborts);
            var tasks = new Task<Balance>[numConcurrentReads];
            for (int i = 0; i < numConcurrentReads; i++)
            {
                var ctx = new TransactionContext(i+1);
                var task = state.Read(ctx);
                tasks[i] = task;
                Assert.IsTrue(task.IsCompleted);
                Assert.IsTrue(task.Result.value == 100000);                
            }

            var ctx0 = new TransactionContext(0);
            var task0 = state.ReadWrite(ctx0);
            Assert.IsFalse(task0.IsCompleted);


            for (int i = 1; i <= numConcurrentReads; i++)
            {
                if (idsToAbort.Contains(i))
                    await state.Abort(i);
                else
                    await state.Commit(i);
            }

            var state0 = await task0;
            state0.value += 100;
            await state.Commit(0);

            for (int i = 0; i < numConcurrentReads; i++)
            {
                var ctx = new TransactionContext(i + 1);
                var task = state.Read(ctx);
                tasks[i] = task;
                Assert.IsTrue(task.IsCompleted);
                Assert.IsTrue(task.Result.value == 100100);
            }

            idsToAbort = GenerateRandomIds(1, numConcurrentReads, numAborts);
            for (int i = 1; i <= numConcurrentReads; i++)
            {
                if (idsToAbort.Contains(i))
                    await state.Abort(i);
                else
                    await state.Commit(i);
            }

            var ctx1 = new TransactionContext(1);
            var task1 = state.Read(ctx1);
            Assert.IsTrue(task0.IsCompleted);
            var state1 = await task1;
            Assert.IsTrue(state1.value == 100100);
            await state.Commit(1);
        }
    }
}
