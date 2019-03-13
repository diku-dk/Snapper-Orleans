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
        ClientConfiguration config;
        IClusterClient client;
        readonly uint numOfCoordinators = 10;

        [TestMethod]
        public void test()
        {
            Assert.AreEqual(42,42);
        }

        [TestInitialize]
        public async Task bootStrap()
        {
            if(config == null)
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
            }            
        }


        private async void TestTransfers(int numTransfers=1, bool sequential=true)
        {
            
        }
        
        [TestMethod]
        public async Task TestSingleDetTransfer()
        {
            Console.WriteLine("Event 1");
            IAccountGrain fromAccount = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(1));
            IAccountGrain toAccount = client.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(2));
            IATMGrain atm = client.GetGrain<IATMGrain>(Helper.convertUInt32ToGuid(3));
            Console.WriteLine("Event 2");

            Guid fromId = fromAccount.GetPrimaryKey();
            Guid toId = toAccount.GetPrimaryKey();
            Guid atmId = atm.GetPrimaryKey();
            Console.WriteLine("Event 3");
            var grainAccessInformation = new Dictionary<Guid, Tuple<String, int>>();
            grainAccessInformation.Add(fromId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
            grainAccessInformation.Add(toId, new Tuple<string, int>("AccountTransfer.Grains.AccountGrain", 1));
            grainAccessInformation.Add(atmId, new Tuple<string, int>("AccountTransfer.Grains.ATMGrain", 1));
            Console.WriteLine("Event 4");
            var args = new TransferInput(1, 2, 10);
            FunctionInput input = new FunctionInput(args);

            //Deterministic Transactions
            var t1 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
            Console.WriteLine("Event 5");
            var t2 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
            Console.WriteLine("Event 6");
            var t3 = atm.StartTransaction(grainAccessInformation, "Transfer", input);
            Console.WriteLine("Event 7");
            await Task.WhenAll(t1, t2, t3);
            Console.WriteLine("Event 7");            
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
