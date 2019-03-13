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
        ClientConfiguration config = new ClientConfiguration();
        IClusterClient client;
        readonly uint numOfCoordinators = 10;

        [TestInitialize]
        public async void bootStrap()
        {
            client = await config.StartClientWithRetries();
            var tasks = new List<Task>();
            //Spawn coordinators
            for (uint i = 0; i < numOfCoordinators; i++)
            {
                IGlobalTransactionCoordinator coordinator = client.GetGrain<IGlobalTransactionCoordinator>(Utilities.Helper.convertUInt32ToGuid(i));
                tasks.Add(coordinator.SpawnCoordinator(i, numOfCoordinators));
            }
            await Task.WhenAll(tasks);
        }

        public void TestHybridSequentialTransfer()
        {
            throw new AssertFailedException("Unimplemented");
        }

        private async void TestTransfers(int numTransfers=1, bool sequential=true)
        {
            
        }
        
        [TestMethod]
        public async void TestSingleDetTransfer()
        {
            IAccountGrain fromAccount = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(1));
            IAccountGrain toAccount = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(2));
            IATMGrain atm = client.GetGrain<IATMGrain>(Helper.convertUInt32ToGuid(3));

            Guid fromId = fromAccount.GetPrimaryKey();
            Guid toId = toAccount.GetPrimaryKey();
            Guid atmId = atm.GetPrimaryKey();

            var grainAccessInformation = new Dictionary<Guid, Tuple<String, int>>();
            grainAccessInformation.Add(fromId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
            grainAccessInformation.Add(toId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
            grainAccessInformation.Add(atmId, new Tuple<string, int>("AccountTransfer.Grains.ATMGrain", 1));

            var args = new TransferInput(1, 2, 10);
            FunctionInput input = new FunctionInput(args);

            //Deterministic Transactions
            var t1 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
            var t2 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
            var t3 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
            await Task.WhenAll(t1, t2, t3);                
        }

        [TestMethod]
        public void TestSequentialDetTransfer()
        {
            throw new AssertFailedException("Unimplemented");
        }

        [TestMethod]
        public void TestConcurrentDetTransfer()
        {
            throw new AssertFailedException("Unimplemented");
        }

        [TestMethod]
        public void TestSingleNonDetTransfer()
        {
            throw new AssertFailedException("Unimplemented");
        }

        [TestMethod]
        public void TestSequentialNonDetTransfer()
        {
            throw new AssertFailedException("Unimplemented");
        }
        [TestMethod]
        public void TestConcurrentNonDetTransfer()
        {
            throw new AssertFailedException("Unimplemented");
        }

        [TestMethod]
        public void TestHybridSequentialTransfer()
        {
            throw new AssertFailedException("Unimplemented");
        }

        [TestMethod]
        public void TestHybridConcurrentTransfer()
        {
            throw new AssertFailedException("Unimplemented");
        }
    }
}
