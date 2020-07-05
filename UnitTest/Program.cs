using System;
using Concurrency.Interface.Nondeterministic;
using Concurrency.Implementation;
using Concurrency.Interface;
using Utilities;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;
using System.Linq;

namespace UnitTest
{
    class Program
    {
        static int num_accounts = 3;
        static int base_numTxn = 20;
        //static ConcurrencyType CCtype = ConcurrencyType.TIMESTAMP;
        static ConcurrencyType CCtype = ConcurrencyType.S2PL;

        static int Main(string[] args)
        {
            Console.WriteLine("Start to do transactions. ");
            //return DoTransactions(1, base_numTxn).Result;
            //return DoTransactions(2, base_numTxn * 2).Result;
            //return DoTransactions(3, base_numTxn).Result;
            //return DoTransactions(4, base_numTxn * 3).Result;
            //return DoTransactions(5, base_numTxn * 3).Result;
            //return DoTransactions(6, base_numTxn).Result;
            return DoTransactions(7, base_numTxn * 3).Result;
        }

        private static async Task<int> DoTransactions(int testID, int numTxn)
        {
            var accounts = new ITransactionalState<MyData>[num_accounts];
            for (int i = 0; i < num_accounts; i++) accounts[i] = new HybridState<MyData>(new MyData(), CCtype);
            var expect_res = new int[num_accounts];
            for (int i = 0; i < num_accounts; i++) expect_res[i] = -1;
            var tasks = new List<Task<Boolean>>();
            var tids = new List<int>();
            for (int tid = 0; tid < numTxn; tid++) tids.Add(tid);
            var shuffled_tids = tids.OrderBy(x => Guid.NewGuid()).ToList();

            switch (testID)
            {
                case 1:   // 1000 Transfer from A to B
                    for (int tid = 0; tid < numTxn; tid++) tasks.Add(Transfer(tid, accounts[0], accounts[1]));
                    expect_res[0] = 0;
                    expect_res[1] = 2000;
                    expect_res[2] = 1000;
                    break;
                case 2:   // 1000 Transfer from A to B + 1000 Transfer from B to A
                    for (int i = 0; i < numTxn;i++)
                    {
                        var tid = shuffled_tids[i];
                        if (tid % 2 == 0) tasks.Add(Transfer(i, accounts[0], accounts[1]));
                        else tasks.Add(Transfer(i, accounts[1], accounts[0]));
                    }
                    expect_res[0] = 1000;
                    expect_res[1] = 1000;
                    expect_res[2] = 1000;
                    break;
                case 3:   // 1000 MultiTransfer from A to A and B
                    for (int tid = 0; tid < numTxn; tid++) tasks.Add(MultiTransfer(tid, accounts[0], accounts[1]));
                    expect_res[0] = 0;
                    expect_res[1] = 2000;
                    expect_res[2] = 1000;
                    break;
                case 4:   // 1000 Transfer from A to B + 1000 GetBalance from A + 1000 GrtBalance from B
                    for (int i = 0; i < numTxn; i++)
                    {
                        var tid = shuffled_tids[i];
                        if (tid % 3 == 0) tasks.Add(Transfer(i, accounts[0], accounts[1]));
                        else if (tid % 3 == 1) tasks.Add(BalanceTxn(i, accounts[0]));
                        else tasks.Add(BalanceTxn(i, accounts[1]));
                    }
                    expect_res[0] = 0;
                    expect_res[1] = 2000;
                    expect_res[2] = 1000;
                    break;
                case 5:  // 1000 Transfer from A to B + 1000 Transfer from B to A + 1000 MultiTransfer from A to A and B
                    for (int i = 0; i < numTxn; i++)
                    {
                        var tid = shuffled_tids[i];
                        if (tid % 3 == 0) tasks.Add(Transfer(i, accounts[0], accounts[1]));
                        else if (tid % 3 == 1) tasks.Add(Transfer(i, accounts[1], accounts[0]));
                        else tasks.Add(MultiTransfer(i, accounts[0], accounts[1]));
                    }
                    expect_res[0] = 0;
                    expect_res[1] = 2000;
                    expect_res[2] = 1000;
                    break;
                case 6:  // 1000 MultiTransfer from A to B and C
                    for (int tid = 0; tid < numTxn; tid++) tasks.Add(MultiTransfer(tid, accounts[0], accounts[1], accounts[2]));
                    expect_res[0] = -1000;
                    expect_res[1] = 2000;
                    expect_res[2] = 2000;
                    break;
                case 7:  // 1000 MultiTransfer from A to B and C + 1000 MultiTransfer from B to A and C + 1000 MultiTransfer from C to A and B
                    for (int i = 0; i < numTxn; i++)
                    {
                        var tid = shuffled_tids[i];
                        if (tid % 3 == 0) tasks.Add(MultiTransfer(i, accounts[0], accounts[1], accounts[2]));
                        else if (tid % 3 == 1) tasks.Add(MultiTransfer(i, accounts[1], accounts[0], accounts[2]));
                        else tasks.Add(MultiTransfer(i, accounts[2], accounts[0], accounts[1]));
                    }
                    expect_res[0] = 1000;
                    expect_res[1] = 1000;
                    expect_res[2] = 1000;
                    break;
                default:
                    Console.WriteLine($"Error: Unknown testID");
                    return 0;
            }

            await Task.WhenAll(tasks);
            var numAborts = 0;
            for (int tid = 0; tid < numTxn; tid++) if (!tasks[tid].Result) numAborts++;
            Console.WriteLine($"total numTxn = {numTxn}, numAbort = {numAborts}. ");
            for (int i = 0; i < 3; i++)
            {
                var final_v = await accounts[i].Read(new TransactionContext(1000000));
                if (final_v.balance != expect_res[i]) Console.WriteLine($"account {i}: expect = {expect_res[i]}, real = {final_v.balance}");
            }
            return 0;
        }

        private static async Task<Boolean> TwoPhaseCommit(int tid, List<ITransactionalState<MyData>> participates, Boolean noException)
        {
            var tasks = new List<Task<Boolean>>();
            var ts = new List<Task>();
            if (noException)
            {
                foreach (var node in participates) tasks.Add(node.Prepare(tid));
                await Task.WhenAll(tasks);
                var success = true;
                foreach (var t in tasks) if (!t.Result) success = false;
                if (success)
                {
                    foreach (var node in participates) ts.Add(node.Commit(tid));
                    await Task.WhenAll(ts);
                    return true;
                }
                else
                {
                    foreach (var node in participates) ts.Add(node.Abort(tid));
                    await Task.WhenAll(ts);
                    return false;
                }
            }
            else
            {
                foreach (var node in participates) ts.Add(node.Abort(tid));
                await Task.WhenAll(ts);
                return false;
            }
        }

        private static async Task<Boolean> Transfer(int tid, ITransactionalState<MyData> A, ITransactionalState<MyData> B)
        {
            var A_success = true;
            var B_success = true;
            var ctx = new TransactionContext(tid);
            var participates = new List<ITransactionalState<MyData>>();
            participates.Add(A);
            A_success = await Withdraw(ctx, A, 1);
            if (A_success) B_success = await Deposit(ctx, B, 1);
            else return await TwoPhaseCommit(tid, participates, false);

            participates.Add(B);
            if (A_success && B_success) return await TwoPhaseCommit(tid, participates, true);
            else return await TwoPhaseCommit(tid, participates, false);
        }

        private static async Task<Boolean> MultiTransfer(int tid, ITransactionalState<MyData> A, ITransactionalState<MyData> B)
        {
            var out_A_success = true;
            var ctx = new TransactionContext(tid);
            var participates = new List<ITransactionalState<MyData>>();
            participates.Add(A);
            out_A_success = await Withdraw(ctx, A, 2);
            
            var tasks = new List<Task<Boolean>>();
            if (out_A_success)
            {
                tasks.Add(Deposit(ctx, A, 1));
                tasks.Add(Deposit(ctx, B, 1));
                await Task.WhenAll(tasks);
            }
            else return await TwoPhaseCommit(tid, participates, false);

            participates.Add(B);
            if (tasks[0].Result && tasks[1].Result) return await TwoPhaseCommit(tid, participates, true);
            else return await TwoPhaseCommit(tid, participates, false);
        }

        private static async Task<Boolean> MultiTransfer(int tid, ITransactionalState<MyData> A, ITransactionalState<MyData> B, ITransactionalState<MyData> C)
        {
            var out_A_success = false;
            var ctx = new TransactionContext(tid);
            var participates = new List<ITransactionalState<MyData>>();
            participates.Add(A);
            out_A_success = await Withdraw(ctx, A, 2);
            var tasks = new List<Task<Boolean>>();
            if (out_A_success)
            {
                tasks.Add(Deposit(ctx, B, 1));
                tasks.Add(Deposit(ctx, C, 1));
                await Task.WhenAll(tasks);
            }
            else return await TwoPhaseCommit(tid, participates, false);

            participates.Add(B);
            participates.Add(C);
            if (tasks[0].Result && tasks[1].Result) return await TwoPhaseCommit(tid, participates, true);
            else return await TwoPhaseCommit(tid, participates, false);
        }

        private static async Task<Boolean> BalanceTxn(int tid, ITransactionalState<MyData> A)
        {
            var ctx = new TransactionContext(tid);
            var participates = new List<ITransactionalState<MyData>>();
            participates.Add(A);
            var succeess = await Balance(ctx, A);
            if (succeess) return await TwoPhaseCommit(tid, participates, true);
            else return await TwoPhaseCommit(tid, participates, false);
        }

        private static async Task<Boolean> WithdrawTxn(int tid, ITransactionalState<MyData> A)
        {
            var ctx = new TransactionContext(tid);
            var participates = new List<ITransactionalState<MyData>>();
            participates.Add(A);
            var succeess = await Withdraw(ctx, A, 1);
            if (succeess) return await TwoPhaseCommit(tid, participates, true);
            else return await TwoPhaseCommit(tid, participates, false);
        }

        private static async Task<Boolean> DepositTxn(int tid, ITransactionalState<MyData> A)
        {
            var ctx = new TransactionContext(tid);
            var participates = new List<ITransactionalState<MyData>>();
            participates.Add(A);
            var succeess = await Withdraw(ctx, A, 1);
            if (succeess) return await TwoPhaseCommit(tid, participates, true);
            else return await TwoPhaseCommit(tid, participates, false);
        }

        private static async Task<Boolean> Balance(TransactionContext ctx, ITransactionalState<MyData> account)
        {
            try
            {
                var res = await account.Read(ctx);
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}");
                return false;
            }
        }

        private static async Task<Boolean> Withdraw(TransactionContext ctx, ITransactionalState<MyData> account, int amount)
        {
            try
            {
                var res = await account.ReadWrite(ctx);
                res.balance -= amount;
                Console.WriteLine($"Txn {ctx.transactionID} withdraw.");
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}");
                return false;
            }
        }

        private static async Task<Boolean> Deposit(TransactionContext ctx, ITransactionalState<MyData> account, int amount)
        {
            try
            {
                var res = await account.ReadWrite(ctx);
                res.balance += amount;
                Console.WriteLine($"Txn {ctx.transactionID} deposit.");
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception: {e.Message}");
                return false;
            }
        }
    }

    class MyData : ICloneable
    {
        public int balance;

        public MyData()
        {
            balance = 1000;   // initial value is 1000
        }

        object ICloneable.Clone()
        {
            var clonedMyData = new MyData();
            clonedMyData.balance = balance;
            return clonedMyData;
        }
    }
}
