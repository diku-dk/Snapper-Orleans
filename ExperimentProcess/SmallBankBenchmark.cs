using MathNet.Numerics.Distributions;
using Orleans;
using SmallBank.Interfaces;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Utilities;
using Concurrency.Interface;

namespace ExperimentProcess
{
    public enum TxnType { Balance, DepositChecking, Transfer, TransactSaving, WriteCheck, MultiTransfer }
    public class SmallBankBenchmark : IBenchmark
    {
        IDiscreteDistribution transactionTypeDistribution;
        WorkloadConfiguration config;
        IDiscreteDistribution accountIdDistribution;
        IDiscreteDistribution detDistribution;
        IDiscreteDistribution grainDistribution;
        IDiscreteDistribution transferAmountDistribution;
        IDiscreteDistribution numGrainInMultiTransferDistribution;

        public void generateBenchmark(WorkloadConfiguration workloadConfig)
        {
            config = workloadConfig;
            if (config.distribution == Utilities.Distribution.ZIPFIAN)
            {
                accountIdDistribution = new Zipf(config.zipfianConstant, (int)config.numAccountsPerGroup - 1, new Random());
                grainDistribution = new Zipf(config.zipfianConstant, ((int)config.numAccounts / (int)config.numAccountsPerGroup) - 1, new Random());
            }

            else if (config.distribution == Utilities.Distribution.UNIFORM)
            {
                accountIdDistribution = new DiscreteUniform(0, (int)config.numAccountsPerGroup - 1, new Random());
                grainDistribution = new DiscreteUniform(0, ((int)config.numAccounts / (int)config.numAccountsPerGroup) - 1, new Random());
            }
            transactionTypeDistribution = new DiscreteUniform(0, 99, new Random());
            detDistribution = new DiscreteUniform(0, 99, new Random());
            transferAmountDistribution = new DiscreteUniform(0, 10, new Random());
            numGrainInMultiTransferDistribution = new DiscreteUniform(10, 15, new Random());
        }

        // getBalance, depositChecking, transfer, transacSaving, writeCheck, multiTransfer
        public TxnType nextTransactionType()
        {
            int type = transactionTypeDistribution.Sample();
            int baseCounter = config.mixture[0];
            if (type < baseCounter)
                return TxnType.Balance;
            baseCounter += config.mixture[1];
            if (type < baseCounter)
                return TxnType.DepositChecking;
            baseCounter += config.mixture[2];
            if (type < baseCounter)
                return TxnType.Transfer;
            baseCounter += config.mixture[3];
            if (type < baseCounter)
                return TxnType.TransactSaving;
            baseCounter += config.mixture[4];
            if (type < baseCounter)
                return TxnType.WriteCheck;
            else
                return TxnType.MultiTransfer;
        }

        public Boolean isDet()
        {
            if (config.deterministicTxnPercent == 0) return false;
            else if (config.deterministicTxnPercent == 100) return true;

            var sample = detDistribution.Sample();
            if (sample < config.deterministicTxnPercent) return true;
            else return false;
        }

        public Task<FunctionResult> Execute(IClusterClient client, uint grainId, String functionName, FunctionInput input, Dictionary<Guid, Tuple<String, int>> grainAccessInfo)
        {
            switch (config.grainImplementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerAccountGroupGrain>(Helper.convertUInt32ToGuid(grainId));
                    if (isDet()) return grain.StartTransaction(grainAccessInfo, functionName, input);
                    else return grain.StartTransaction(functionName, input);
                case ImplementationType.ORLEANSEVENTUAL:
                    var eventuallyConsistentGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(Helper.convertUInt32ToGuid(grainId));
                    return eventuallyConsistentGrain.StartTransaction(functionName, input);
                case ImplementationType.ORLEANSTXN:
                    var txnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(Helper.convertUInt32ToGuid(grainId));
                    return txnGrain.StartTransaction(functionName, input);
                default:
                    return null;
            }
        }

        private uint getAccountForGrain(uint grainId)
        {
            return grainId * config.numAccountsPerGroup + (uint)accountIdDistribution.Sample();
        }

        public Task<FunctionResult> newTransaction(IClusterClient client, int global_tid, TxnType type)   // added by Yijian for CC test
        {
            FunctionInput input = null;
            uint groupId = 0;
            Dictionary<Guid, Tuple<String, int>> grainAccessInfo = null;
            if (type != TxnType.Transfer && type != TxnType.MultiTransfer)
            {
                groupId = (uint)grainDistribution.Sample();
                var accountId = getAccountForGrain(groupId);
                switch (type)
                {
                    case TxnType.Balance:
                        input = new FunctionInput(accountId.ToString());
                        break;
                    case TxnType.DepositChecking:
                        Tuple<Tuple<String, UInt32>, float> args1 = new Tuple<Tuple<string, uint>, float>(new Tuple<string, uint>(accountId.ToString(), accountId), 1);
                        input = new FunctionInput(args1);
                        break;
                    case TxnType.TransactSaving:
                        Tuple<String, float> args2 = new Tuple<string, float>(accountId.ToString(), 1);
                        input = new FunctionInput(args2);
                        break;
                    case TxnType.WriteCheck:
                        Tuple<String, float> args3 = new Tuple<string, float>(accountId.ToString(), 1);
                        input = new FunctionInput(args3);
                        break;
                    default:
                        break;
                }
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
            }
            else if (type == TxnType.Transfer)
            {
                groupId = (uint)grainDistribution.Sample();
                var sourceAccountId = getAccountForGrain(groupId);
                var item1 = new Tuple<string, uint>(sourceAccountId.ToString(), sourceAccountId);
                uint destinationId;
                do destinationId = (uint)grainDistribution.Sample();
                while (groupId == destinationId);    // find a group which is not source group
                var destinationAccountId = getAccountForGrain(destinationId);
                Tuple<String, UInt32> item2 = new Tuple<string, uint>(destinationAccountId.ToString(), destinationAccountId);
                float item3 = 1;
                Tuple<Tuple<String, UInt32>, Tuple<String, UInt32>, float> args = new Tuple<Tuple<string, uint>, Tuple<string, uint>, float>(item1, item2, item3);
                input = new FunctionInput(args);
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(destinationId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
            }
            else
            {
                var numGrain = numGrainInMultiTransferDistribution.Sample();
                var accountGrains = new HashSet<uint>();
                do accountGrains.Add((uint)grainDistribution.Sample());
                //while (accountGrains.Count != config.numGrainsMultiTransfer);
                while (accountGrains.Count != numGrain);
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                Tuple<String, UInt32> item1 = null;
                float item2 = 1;         // source account transfer item2 for every destination account
                List<Tuple<string, uint>> item3 = new List<Tuple<String, uint>>();
                bool first = true;
                foreach (var item in accountGrains)
                {
                    if (first)
                    {
                        first = false;
                        groupId = item;
                        grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                        uint sourceId = getAccountForGrain(item);
                        item1 = new Tuple<String, uint>(sourceId.ToString(), sourceId);
                        continue;
                    }
                    uint destAccountId = getAccountForGrain(item);
                    item3.Add(new Tuple<string, uint>(destAccountId.ToString(), destAccountId));
                    grainAccessInfo.Add(Helper.convertUInt32ToGuid(item), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                }
                var args = new Tuple<Tuple<String, UInt32>, float, List<Tuple<String, UInt32>>>(item1, item2, item3);
                input = new FunctionInput(args);
            }
            input.context = new TransactionContext(global_tid);   // added by Yijian
            var task = Execute(client, groupId, type.ToString(), input, grainAccessInfo);
            return task;
        }

        public Task<FunctionResult> newTransaction(IClusterClient client, int global_tid)   // Yijian add gloal_tid
        {
            var type = nextTransactionType();
            FunctionInput input = null;
            uint groupId = 0;
            Dictionary<Guid, Tuple<String, int>> grainAccessInfo = null;
            if (type != TxnType.Transfer && type != TxnType.MultiTransfer)
            {
                groupId = (uint)grainDistribution.Sample();
                var accountId = getAccountForGrain(groupId);
                switch (type)
                {
                    case TxnType.Balance:
                        input = new FunctionInput(accountId.ToString());
                        break;
                    case TxnType.DepositChecking:
                        Tuple<Tuple<String, UInt32>, float> args1 = new Tuple<Tuple<string, uint>, float>(new Tuple<string, uint>(accountId.ToString(), accountId), transferAmountDistribution.Sample());
                        input = new FunctionInput(args1);
                        break;
                    case TxnType.TransactSaving:
                        Tuple<String, float> args2 = new Tuple<string, float>(accountId.ToString(), transferAmountDistribution.Sample());
                        input = new FunctionInput(args2);
                        break;
                    case TxnType.WriteCheck:
                        Tuple<String, float> args3 = new Tuple<string, float>(accountId.ToString(), transferAmountDistribution.Sample());
                        input = new FunctionInput(args3);
                        break;
                    default:
                        break;
                }
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
            }
            else if (type == TxnType.Transfer)
            {
                groupId = (uint)grainDistribution.Sample();
                var sourceAccountId = getAccountForGrain(groupId);
                var item1 = new Tuple<string, uint>(sourceAccountId.ToString(), sourceAccountId);
                uint destinationId;
                do destinationId = (uint)grainDistribution.Sample();
                while (groupId == destinationId);    // find a group which is not source group
                var destinationAccountId = getAccountForGrain(destinationId);
                Tuple<String, UInt32> item2 = new Tuple<string, uint>(destinationAccountId.ToString(), destinationAccountId);
                float item3 = (uint)transferAmountDistribution.Sample();
                Tuple<Tuple<String, UInt32>, Tuple<String, UInt32>, float> args = new Tuple<Tuple<string, uint>, Tuple<string, uint>, float>(item1, item2, item3);
                input = new FunctionInput(args);
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(destinationId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
            }
            else
            {
                var numGrain = numGrainInMultiTransferDistribution.Sample();
                var accountGrains = new HashSet<uint>();
                do accountGrains.Add((uint)grainDistribution.Sample());
                //while (accountGrains.Count != config.numGrainsMultiTransfer);
                while (accountGrains.Count != numGrain);
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                Tuple<String, UInt32> item1 = null;
                float item2 = transferAmountDistribution.Sample();
                List<Tuple<string, uint>> item3 = new List<Tuple<String, uint>>();
                bool first = true;
                foreach (var item in accountGrains)
                {
                    if (first)
                    {
                        first = false;
                        groupId = item;
                        grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                        uint sourceId = getAccountForGrain(item);
                        item1 = new Tuple<String, uint>(sourceId.ToString(), sourceId);
                        continue;
                    }
                    uint destAccountId = getAccountForGrain(item);
                    item3.Add(new Tuple<string, uint>(destAccountId.ToString(), destAccountId));
                    grainAccessInfo.Add(Helper.convertUInt32ToGuid(item), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                }
                var args = new Tuple<Tuple<String, UInt32>, float, List<Tuple<String, UInt32>>>(item1, item2, item3);
                input = new FunctionInput(args);
            }
            input.context = new TransactionContext(global_tid);   // added by Yijian
            var task = Execute(client, groupId, type.ToString(), input, grainAccessInfo);
            return task;
        }

        public Task<FunctionResult> newTransaction(IClusterClient client)   // Yijian add gloal_tid
        {
            var type = nextTransactionType();
            FunctionInput input = null;
            uint groupId = 0;
            Dictionary<Guid, Tuple<String, int>> grainAccessInfo = null;
            if (type != TxnType.Transfer && type != TxnType.MultiTransfer)
            {
                groupId = (uint)grainDistribution.Sample();
                var accountId = getAccountForGrain(groupId);
                switch (type)
                {
                    case TxnType.Balance:
                        input = new FunctionInput(accountId.ToString());
                        break;
                    case TxnType.DepositChecking:
                        Tuple<Tuple<String, UInt32>, float> args1 = new Tuple<Tuple<string, uint>, float>(new Tuple<string, uint>(accountId.ToString(), accountId), 1);
                        input = new FunctionInput(args1);
                        break;
                    case TxnType.TransactSaving:
                        Tuple<String, float> args2 = new Tuple<string, float>(accountId.ToString(), 1);
                        input = new FunctionInput(args2);
                        break;
                    case TxnType.WriteCheck:
                        Tuple<String, float> args3 = new Tuple<string, float>(accountId.ToString(), 1);
                        input = new FunctionInput(args3);
                        break;
                    default:
                        break;
                }
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
            }
            else if (type == TxnType.Transfer)
            {
                groupId = (uint)grainDistribution.Sample();
                var sourceAccountId = getAccountForGrain(groupId);
                var item1 = new Tuple<string, uint>(sourceAccountId.ToString(), sourceAccountId);
                uint destinationId;
                do destinationId = (uint)grainDistribution.Sample();
                while (groupId == destinationId);    // find a group which is not source group
                var destinationAccountId = getAccountForGrain(destinationId);
                Tuple<String, UInt32> item2 = new Tuple<string, uint>(destinationAccountId.ToString(), destinationAccountId);
                float item3 = (uint)1;
                Tuple<Tuple<String, UInt32>, Tuple<String, UInt32>, float> args = new Tuple<Tuple<string, uint>, Tuple<string, uint>, float>(item1, item2, item3);
                input = new FunctionInput(args);
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                grainAccessInfo.Add(Helper.convertUInt32ToGuid(destinationId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
            }
            else
            {
                var numGrain = numGrainInMultiTransferDistribution.Sample();
                var accountGrains = new HashSet<uint>();
                do accountGrains.Add((uint)grainDistribution.Sample());
                //while (accountGrains.Count != config.numGrainsMultiTransfer);
                while (accountGrains.Count != numGrain);
                grainAccessInfo = new Dictionary<Guid, Tuple<string, int>>();
                Tuple<String, UInt32> item1 = null;
                float item2 = 1;
                List<Tuple<string, uint>> item3 = new List<Tuple<String, uint>>();
                bool first = true;
                foreach (var item in accountGrains)
                {
                    if (first)
                    {
                        first = false;
                        groupId = item;
                        grainAccessInfo.Add(Helper.convertUInt32ToGuid(groupId), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                        uint sourceId = getAccountForGrain(item);
                        item1 = new Tuple<String, uint>(sourceId.ToString(), sourceId);
                        continue;
                    }
                    uint destAccountId = getAccountForGrain(item);
                    item3.Add(new Tuple<string, uint>(destAccountId.ToString(), destAccountId));
                    grainAccessInfo.Add(Helper.convertUInt32ToGuid(item), new Tuple<String, int>("SmallBank.Grains.CustomerAccountGroupGrain", 1));
                }
                var args = new Tuple<Tuple<String, UInt32>, float, List<Tuple<String, UInt32>>>(item1, item2, item3);
                input = new FunctionInput(args);
            }
            var task = Execute(client, groupId, type.ToString(), input, grainAccessInfo);
            return task;
        }
    }
}