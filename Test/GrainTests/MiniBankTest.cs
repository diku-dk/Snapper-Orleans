using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Concurrency.Interface;
using AccountTransfer.Interfaces;
using Concurrency.Interface.Nondeterministic;
using Utilities;
using Orleans.Core;
using SmallBank.Interfaces;

namespace Test.GrainTests
{
    [TestClass]
    public class MiniBankTest
    {
        static ClientConfiguration config;
        static IClusterClient client;
        static IConfigurationManagerGrain configGrain;
        Random rand = new Random();
        static readonly uint numOfCoordinators = 5;
        static readonly int batchIntervalMsecs = 1000;
        static readonly int backoffIntervalMsecs = 1000;
        static readonly int idleIntervalTillBackOffSecs = 10;
        static readonly uint numAccountsPerGrain = 100;
        static readonly uint numGroup = 100;
        readonly uint maxParticipantsPerTransfer = 64;
        readonly int maxTransferAmount = 10;
        readonly int numSequentialTransfers = 10;
        readonly int numConcurrentTransfers = 1000;
        static readonly int maxNonDetWaitingLatencyInMs = 1000;
        static readonly ConcurrencyType nonDetCCType = ConcurrencyType.TIMESTAMP;

        [ClassInitialize]
        public static async Task ClassInitialize(TestContext context)
        {
            if (config == null)
            {
                config = new ClientConfiguration();
                client = await config.StartClientWithRetries();
                //Spawn Configuration grain
                configGrain = client.GetGrain<IConfigurationManagerGrain>(Helper.convertUInt32ToGuid(0));
                var exeConfig = new ExecutionGrainConfiguration(new LoggingConfiguration(), new ConcurrencyConfiguration(nonDetCCType), maxNonDetWaitingLatencyInMs);
                var coordConfig = new CoordinatorGrainConfiguration(batchIntervalMsecs, backoffIntervalMsecs, idleIntervalTillBackOffSecs, numOfCoordinators);
                await configGrain.UpdateNewConfiguration(exeConfig);
                await configGrain.UpdateNewConfiguration(coordConfig);
                await InitializeGroups();
            }
        }

        static async Task<Boolean> InitializeGroups()
        {
            List<Task<FunctionResult>> tasks = new List<Task<FunctionResult>>();
            for (uint i = 0; i < numGroup; i++)
            {
                var args = new Tuple<uint, uint>(numAccountsPerGrain, i);
                var input = new FunctionInput(args);
                var groupGUID = Helper.convertUInt32ToGuid(i);
                ICustomerAccountGroupGrain destination = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
                tasks.Add(destination.StartTransaction("InitBankAccounts", input));
            }
            await Task.WhenAll(tasks);
            foreach(Task<FunctionResult> task in tasks){
                bool exception = task.Result.hasException();
                if (exception)
                    return false;
            }
            return true;
        }


        [TestMethod]
        public async Task SingleMultiTransferTest()
        {
            uint numDestinationAccount = 8;
            uint sourceID = 201;
            Tuple<String, UInt32> item1 = new Tuple<string, uint>(sourceID.ToString(), sourceID);
            float item2 = 10;
            List<Tuple<string, uint>> item3 = new List<Tuple<string, uint>>();
            List<uint> destinationIDs = new List<uint>();
            for(uint i = 0; i < numDestinationAccount; i++)
            {
                uint destAccountID = 301;
                item3.Add(new Tuple<string, uint>(destAccountID.ToString(), destAccountID));
            }
            var args = new Tuple<Tuple<String, UInt32>, float, List<Tuple<String, UInt32>>>(item1, item2, item3);
            var input = new FunctionInput(args);
            var groupGUID = Helper.convertUInt32ToGuid(sourceID / numAccountsPerGrain);
            var destination = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
            Task<FunctionResult> task = destination.StartTransaction("MultiTransfer", input);
            await task;
            Assert.AreEqual(true, !task.Result.hasException());
        }

        [TestMethod]
        public async Task SingleBalenceTest()
        {
            uint sourceID = 201;
            string sourceName = sourceID.ToString();
            var input = new FunctionInput(sourceName);
            var groupGUID = Helper.convertUInt32ToGuid(sourceID / numAccountsPerGrain);
            var destination = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
            Task<FunctionResult> task = destination.StartTransaction("Balance", input);
            await task;
            Assert.AreEqual(true, !task.Result.hasException());
        }

        [TestMethod]
        public async Task SingleTransferTest()
        {
 
            uint sourceID = 201;
            Tuple<String, UInt32> item1 = new Tuple<string, uint>(sourceID.ToString(), sourceID);
            uint destID = 301;
            Tuple<String, UInt32> item2 = new Tuple<string, uint>(destID.ToString(), destID);

            float item3 = 10;
            Tuple<Tuple<String, UInt32>, Tuple<String, UInt32>, float> args = new Tuple<Tuple<string, uint>, Tuple<string, uint>, float>(item1, item2, item3);

            var input = new FunctionInput(args);
            var groupGUID = Helper.convertUInt32ToGuid(sourceID / numAccountsPerGrain);
            var destination = client.GetGrain<ICustomerAccountGroupGrain>(groupGUID);
            Task<FunctionResult> task = destination.StartTransaction("Transfer", input);
            await task;
            Assert.AreEqual(true, !task.Result.hasException());
        }


    }
}
