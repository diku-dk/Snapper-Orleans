using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Concurrency.Interface;
using AccountTransfer.Interfaces;
using Utilities;
using Orleans.Core;

namespace Test.GrainTests
{
    [TestClass]
    public class AccountGrainTest
    {
        static ClientConfiguration config;
        static IClusterClient client;
        Random rand = new Random();
        static readonly uint numOfCoordinators = 5;
        static readonly int maxAccounts = 10;
        readonly int maxTransferAmount = 10;
        readonly int numSequentialTransfers = 10;
        readonly int numConcurrentTransfers = 1000;

        [ClassInitialize]
        public static async Task ClassInitialize(TestContext context)
        {
            if (config == null)
            {
                config = new ClientConfiguration();
                client = await config.StartClientWithRetries();
                var tasks = new List<Task>();
                //Spawn coordinators
                for (uint i = 0; i < numOfCoordinators; i++)
                {
                    IGlobalTransactionCoordinator coordinator = client.GetGrain<IGlobalTransactionCoordinator>(Utilities.Helper.convertUInt32ToGuid(i));
                    tasks.Add(coordinator.SpawnCoordinator(i, numOfCoordinators));
                }
                await Task.WhenAll(tasks);
                IGlobalTransactionCoordinator coord_0 = client.GetGrain<IGlobalTransactionCoordinator>(Utilities.Helper.convertUInt32ToGuid(0));
                BatchToken token = new BatchToken(-1, -1);
                await coord_0.PassToken(token);
            }
        }

        [TestInitialize]
        public async Task bootStrap()
        {
           
        }

        private List<Tuple<uint, uint, float, bool>> GenerateTransferInformation(int numTuples, Tuple<int, int> fromAccountRange, Tuple<int, int> toAccountRange, Tuple<int, int> transferAmountRange, Tuple<bool, bool> hybridTypeRange)
        {
            var transferInformation = new List<Tuple<uint, uint, float, bool>>();
            while (numTuples-- > 0)
            {
                var fromAccount = (uint)rand.Next(fromAccountRange.Item1, fromAccountRange.Item2);
                var toAccount = (uint)rand.Next(toAccountRange.Item1, toAccountRange.Item2);
                while (fromAccount == toAccount)
                {
                    if ((fromAccountRange.Item2 - fromAccountRange.Item1) > (toAccountRange.Item2 - toAccountRange.Item1))
                    {
                        fromAccount = (uint)rand.Next(fromAccountRange.Item1, fromAccountRange.Item2);
                    }
                    else
                    {
                        toAccount = (uint)rand.Next(toAccountRange.Item1, toAccountRange.Item2);
                    }
                }

                var transferAmount = (float)rand.Next(transferAmountRange.Item1, transferAmountRange.Item2);
                bool transactionType = (hybridTypeRange.Item1 == hybridTypeRange.Item2) ? hybridTypeRange.Item1 : Convert.ToBoolean(rand.Next(0, 1));
                transferInformation.Add(new Tuple<uint, uint, float, bool>(fromAccount, toAccount, transferAmount, transactionType));
            }
            return transferInformation;
        }


        private async Task TestTransfers(bool sequential, List<Tuple<uint,uint,float, bool>> transferInformation)
        {
            //Read balances prior to transfer
            var accountBalances = new Dictionary<Guid, float>();
            var balanceTaskInfo = new List<Tuple<Guid, Task<FunctionResult>>>();
            var balanceTasks = new List<Task<FunctionResult>>();

            foreach (var transferInfoTuple in transferInformation)
            {
                var fromId = Helper.convertUInt32ToGuid(transferInfoTuple.Item1);
                var toId = Helper.convertUInt32ToGuid(transferInfoTuple.Item2);
                var fromAccount = client.GetGrain<IAccountGrain>(fromId);
                var toAccount = client.GetGrain<IAccountGrain>(toId);
                Task<FunctionResult> t1 = fromAccount.StartTransaction("GetBalance", new FunctionInput());
                balanceTaskInfo.Add(new Tuple<Guid, Task<FunctionResult>>(fromId, t1));
                balanceTasks.Add(t1);
                Task<FunctionResult> t2 = toAccount.StartTransaction("GetBalance", new FunctionInput());
                balanceTaskInfo.Add(new Tuple<Guid, Task<FunctionResult>>(toId, t2));
                balanceTasks.Add(t2);
            }
            await Task.WhenAll(balanceTasks);
            foreach (var aBalanceTaskInfo in balanceTaskInfo)
            {
                if (aBalanceTaskInfo.Item2.Result.hasException())
                {
                    Assert.Fail();
                }
                Assert.IsFalse(aBalanceTaskInfo.Item2.Result.hasException());
                accountBalances[aBalanceTaskInfo.Item1] = (float)aBalanceTaskInfo.Item2.Result.resultObject;
            }
            
            var taskInfo = new List<Tuple<Guid, Guid, float, Task<FunctionResult>>>();
            var tasks = new List<Task<FunctionResult>>();
            foreach (var transferInfoTuple in transferInformation)
            {
                var fromId = Helper.convertUInt32ToGuid(transferInfoTuple.Item1);
                var toId = Helper.convertUInt32ToGuid(transferInfoTuple.Item2);
                var fromAccount = client.GetGrain<IAccountGrain>(fromId);
                var toAccount = client.GetGrain<IAccountGrain>(toId);

                var args = new TransferInput(transferInfoTuple.Item1, transferInfoTuple.Item2, transferInfoTuple.Item3);
                var input = new FunctionInput(args);
                Task<FunctionResult> task;
                if (transferInfoTuple.Item4)
                {
                    //Deterministic transaction
                    var grainAccessInformation = new Dictionary<Guid, Tuple<String, int>>();
                    grainAccessInformation.Add(fromId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
                    grainAccessInformation.Add(toId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
                    task = fromAccount.StartTransaction(grainAccessInformation, "Transfer", input);                    
                } else
                {
                    //Non-deterministic transaction
                    task = fromAccount.StartTransaction("Transfer", input);
                }
                taskInfo.Add(new Tuple<Guid, Guid, float, Task<FunctionResult>>(fromId, toId, transferInfoTuple.Item3, task));
                tasks.Add(task);
                if (sequential)
                {
                    await task;
                    Assert.IsFalse(task.Result.hasException());
                }
            }
            if(!sequential)
            {
                await Task.WhenAll(tasks);
            }

            foreach (var aTaskInfo in taskInfo)
            {
                if (!aTaskInfo.Item4.Result.hasException())
                {
                    accountBalances[aTaskInfo.Item1] -= aTaskInfo.Item3;
                    accountBalances[aTaskInfo.Item2] += aTaskInfo.Item3;
                }
            }

            balanceTaskInfo = new List<Tuple<Guid, Task<FunctionResult>>>();
            balanceTasks = new List<Task<FunctionResult>>();
            foreach (var transferInfoTuple in transferInformation)
            {
                var fromId = Helper.convertUInt32ToGuid(transferInfoTuple.Item1);
                var toId = Helper.convertUInt32ToGuid(transferInfoTuple.Item2);
                var fromAccount = client.GetGrain<IAccountGrain>(fromId);
                var toAccount = client.GetGrain<IAccountGrain>(toId);
                Task<FunctionResult> t1 = fromAccount.StartTransaction("GetBalance", new FunctionInput());
                balanceTaskInfo.Add(new Tuple<Guid, Task<FunctionResult>>(fromId, t1));
                balanceTasks.Add(t1);
                Task<FunctionResult> t2 = toAccount.StartTransaction("GetBalance", new FunctionInput());
                balanceTaskInfo.Add(new Tuple<Guid, Task<FunctionResult>>(toId, t2));
                balanceTasks.Add(t2);
            }
            await Task.WhenAll(balanceTasks);
            foreach (var aBalanceTaskInfo in balanceTaskInfo)
            {
                Assert.IsFalse(aBalanceTaskInfo.Item2.Result.hasException());
                Assert.IsTrue((float)aBalanceTaskInfo.Item2.Result.resultObject == accountBalances[aBalanceTaskInfo.Item1]);
                accountBalances[aBalanceTaskInfo.Item1] = (float)aBalanceTaskInfo.Item2.Result.resultObject;
            }
        }
        
        [TestMethod]
        public async Task TestSingleDetTransfer()
        {
            var transferInfo = GenerateTransferInformation(1, new Tuple<int, int>(0, maxAccounts/2), new Tuple<int, int>((maxAccounts/2)+1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(true, true));
            await TestTransfers(true, transferInfo);
        }

        [TestMethod]
        public async Task TestSequentialDetTransfer()
        {
            var transferInfo = GenerateTransferInformation(numSequentialTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(true, true));
            await TestTransfers(true, transferInfo);
        }

        [TestMethod]
        public async Task TestConcurrentDetTransfer()
        {
            var transferInfo = GenerateTransferInformation(numConcurrentTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(true, true));
            await TestTransfers(false, transferInfo);
        }

        [TestMethod]
        public async Task TestSingleNonDetTransfer()
        {
            var transferInfo = GenerateTransferInformation(1, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(false, false));
            await TestTransfers(true, transferInfo);
        }

        [TestMethod]
        public async Task TestSequentialNonDetTransfer()
        {
            var transferInfo = GenerateTransferInformation(numSequentialTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(false, false));
            await TestTransfers(true, transferInfo);
        }
        [TestMethod]
        public async Task TestConcurrentNonDetTransfer()
        {
            var transferInfo = GenerateTransferInformation(numConcurrentTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(false, false));
            await TestTransfers(false, transferInfo);            
        }

        [TestMethod]
        public async Task TestHybridSequentialTransfer()
        {
            var transferInfo = GenerateTransferInformation(numSequentialTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(false, true));
            await TestTransfers(true, transferInfo);
        }

        [TestMethod]
        public async Task TestHybridConcurrentTransfer()
        {
            var transferInfo = GenerateTransferInformation(numSequentialTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(false, true));
            await TestTransfers(false, transferInfo);
        }

        [TestMethod]
        public async Task TestDetThenNonDetConcurrentTransfer()
        {
            var transferInfo_det = GenerateTransferInformation(numSequentialTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(true, true));
            await TestTransfers(false, transferInfo_det);

            var transferInfo_nondet = GenerateTransferInformation(numSequentialTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(false, false));
            await TestTransfers(false, transferInfo_nondet);
        }

        [TestMethod]
        public async Task TestNonDetThenDetConcurrentTransfer()
        {
            var transferInfo_nondet = GenerateTransferInformation(numSequentialTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(false, false));
            await TestTransfers(false, transferInfo_nondet);

            var transferInfo_det = GenerateTransferInformation(numSequentialTransfers, new Tuple<int, int>(0, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(true, true));
            await TestTransfers(false, transferInfo_det);
        }
    }
}
