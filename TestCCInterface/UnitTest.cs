using System;
using Xunit;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using Utilities;
using Concurrency.Implementation;
using System.Threading.Tasks;

namespace TestCCInterface
{
    public class UnitTest
    {
        ITransactionalState<Balance> state;

        [Fact]
        public async Task TestS2PL()
        {
            // T3.R  T1.R  T5.R  T2.W (T2 will be aborted)
            state = new HybridState<Balance>(new Balance(), ConcurrencyType.S2PL);
            var task1 = state.Read(new TransactionContext(3));
            var task2 = state.Read(new TransactionContext(1));
            var task3 = state.Read(new TransactionContext(5));
            var task4 = state.Read(new TransactionContext(3));   // R and then R is allowed
            Assert.True(task1.IsCompleted && task2.IsCompleted && task3.IsCompleted && task4.IsCompleted);
            try
            {
                await state.ReadWrite(new TransactionContext(2));
                Assert.True(false);
            }
            catch (DeadlockAvoidanceException)
            {
                Assert.True(true);
            }
            // T3.R  T1.R  T5.R  T1.W (T1 will be aborted because lock upgrade is not allowed)
            try
            {
                await state.ReadWrite(new TransactionContext(1));
                Assert.True(false);
            }
            catch (DeadlockAvoidanceException)
            {
                Assert.True(true);
            }
            // T3.R  T1.R  T5.R  T0.W
            var task5 = state.ReadWrite(new TransactionContext(0));
            await state.Commit(1);
            await state.Commit(3);
            // T5.R  T4.R  T0.W (T4 can be inserted before T0)
            var task6 = state.Read(new TransactionContext(4));
            Assert.True(task6.IsCompleted);
            // T5.R  T4.R  T2.W  T0.W
            var task7 = state.ReadWrite(new TransactionContext(2));
            await state.Commit(5);
            await state.Commit(4);
            Assert.True(task7.IsCompleted);
            // T2.W  T0.W
            var task8 = state.Read(new TransactionContext(2));   // RW and then R is allowed
            Assert.True(task8.IsCompleted);
            var task9 = state.ReadWrite(new TransactionContext(2));  // RW and then RW is allowed
            Assert.True(task9.IsCompleted);
            await state.Commit(2);
            Assert.True(task5.IsCompleted);
            await state.Commit(0);
        }

        [Fact]
        public async Task TestTimestamp()
        {
            // T3.R  T3.W
            state = new HybridState<Balance>(new Balance(), ConcurrencyType.TIMESTAMP);
            var task1 = state.Read(new TransactionContext(3));
            var task2 = state.ReadWrite(new TransactionContext(3));  // R and then RW is allowed
            Assert.True(task1.IsCompleted && task2.IsCompleted);
            // T3.R  T3.W  T2.R (T2 will be aborted) 
            try
            {
                await state.Read(new TransactionContext(2));
                Assert.True(false);
            }
            catch (DeadlockAvoidanceException)
            {
                Assert.True(true);
            }
            // T3.R  T3.W  T6.R
            var task3 = state.Read(new TransactionContext(6));
            Assert.True(task3.IsCompleted);
            // T3.R  T3.W  T6.R  T6.R (R then R is allowed)
            var task = state.Read(new TransactionContext(6));
            Assert.True(task.IsCompleted);
            // T3.R  T3.W  T6.R  T6.R  T5.R
            var task4 = state.Read(new TransactionContext(5));
            Assert.True(task4.IsCompleted);
            // T3.R  T3.W  T6.R  T6.R  T5.R  T4.W (T4 will be aborted)
            try
            {
                await state.ReadWrite(new TransactionContext(4));
                Assert.True(false);
            }
            catch (DeadlockAvoidanceException)
            {
                Assert.True(true);
            }
            // T3.R  T3.W  T6.R  T6.R  T5.R  T3.R  (RW then R is allowed)
            var task5 = state.Read(new TransactionContext(3));
            Assert.True(task5.IsCompleted);
            var task6 = state.Prepare(6);
            Assert.True(!task6.IsCompleted);
            var task7 = state.Prepare(3);
            var task8 = state.Commit(3);
            Assert.True(task8.IsCompleted && task7.IsCompleted && task6.IsCompleted);
            await state.Commit(6);
            await state.Commit(5);
            // then T7.RW  T8.R
            
            var task9 = state.ReadWrite(new TransactionContext(7));
            var task10 = state.Read(new TransactionContext(8));
            // T7.RW  T8.R  T7.RW (T7 will be aborted)
            try
            {
                await state.ReadWrite(new TransactionContext(7));
                Assert.True(false);
            }
            catch (DeadlockAvoidanceException)
            {
                Assert.True(true);
            }
            var task11 = state.Prepare(8);
            Assert.True(!task11.IsCompleted);
            var task12 = state.Abort(7);
            Assert.True(task12.IsCompleted && task11.IsCompleted && !task11.Result);
            await state.Abort(8);
        }
    }

    public class Balance : ICloneable
    {
        public int balance;

        public Balance()
        {
            balance = 1000;
        }

        object ICloneable.Clone()
        {
            var clonedBalance = new Balance();
            clonedBalance.balance = balance;
            return clonedBalance;
        }
    }
}
