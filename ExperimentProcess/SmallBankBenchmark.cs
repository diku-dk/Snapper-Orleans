using System;
using Orleans;
using Utilities;
using System.Linq;
using SmallBank.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;
using MathNet.Numerics.Distributions;

namespace ExperimentProcess
{
    public enum TxnType { Balance, DepositChecking, Transfer, TransactSaving, WriteCheck, MultiTransfer }
    public class SmallBankBenchmark : IBenchmark
    {
        private WorkloadConfiguration config;
        private IDiscreteDistribution detDistribution;
        private IDiscreteDistribution grainDistribution;
        private IDiscreteDistribution accountIdDistribution;
        private IDiscreteDistribution transferAmountDistribution;
        private IDiscreteDistribution transactionTypeDistribution;
        private IDiscreteDistribution numGrainInMultiTransferDistribution;
        private Dictionary<int, List<int>> grainIDs = new Dictionary<int, List<int>>();  // <txn index, grains accessed by the txn>

        private double hotGrainRatio;
        private IDiscreteDistribution hotGrainDistribution;
        private IDiscreteDistribution normalGrainDistribution;

        private int index = 0;

        public void setIndex(int num)
        {
            index = num;
        }

        public void generateBenchmark(WorkloadConfiguration workloadConfig, double skew, double hotGrainRatio)
        {
            config = workloadConfig;
            this.hotGrainRatio = hotGrainRatio;
            var numGrain = config.numAccounts / config.numAccountsPerGroup;
            accountIdDistribution = new DiscreteUniform(0, config.numAccountsPerGroup - 1, new Random());
            switch (config.distribution)
            {
                case Distribution.UNIFORM:
                    grainDistribution = new DiscreteUniform(0, numGrain - 1, new Random());
                    break;
                case Distribution.ZIPFIAN:
                    grainDistribution = new Zipf(config.zipfianConstant, numGrain - 1, new Random());
                    break;
                case Distribution.HOTRECORD:
                    var hotGrain = (int)(numGrain * skew);
                    hotGrainDistribution = new DiscreteUniform(0, hotGrain - 1, new Random());
                    if (skew != 1) normalGrainDistribution = new DiscreteUniform(hotGrain, numGrain - 1, new Random());
                    else normalGrainDistribution = hotGrainDistribution;
                    break;
                default:
                    throw new Exception("Exception: Unknown distribution. ");
            }

            detDistribution = new DiscreteUniform(0, 99, new Random());
            transferAmountDistribution = new DiscreteUniform(0, 10, new Random());
            transactionTypeDistribution = new DiscreteUniform(0, 99, new Random());
            numGrainInMultiTransferDistribution = new DiscreteUniform(5, 6, new Random());
        }

        public void generateGrainIDs(int threadIndex, int epoch)
        {
            string line;
            var numGrain = config.numAccounts / config.numAccountsPerGroup;
            var numGrainPerTxn = config.numGrainsMultiTransfer;
            var zipf = config.zipfianConstant;
            string path;
            var suffix = $"_{numGrain}_{zipf}_{threadIndex}_{epoch}.txt";

            if (config.mixture[2] == 100) path = Constants.dataPath + $@"Transfer\Transfer" + suffix;
            else if (config.mixture.Sum() == 0) path = Constants.dataPath + $@"MultiTransfer\{numGrainPerTxn}\{numGrain}\MultiTransfer_{numGrainPerTxn}" + suffix;
            else throw new Exception("only support transfer and multitransfer");
            var grains = new List<int>();
            System.IO.StreamReader file = new System.IO.StreamReader(path);
            while ((line = file.ReadLine()) != null)
            {
                var id = int.Parse(line);
                grains.Add(id);
            }
            file.Close();
            grainIDs.Add(epoch, grains);
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

        public bool isDet()
        {
            if (config.deterministicTxnPercent == 0) return false;
            else if (config.deterministicTxnPercent == 100) return true;

            var sample = detDistribution.Sample();
            if (sample < config.deterministicTxnPercent) return true;
            else return false;
        }

        public Task<TransactionResult> Execute(IClusterClient client, int grainID, string functionName, FunctionInput input, Dictionary<int, int> grainAccessInfo)
        {
            switch (config.grainImplementationType)
            {
                case ImplementationType.SNAPPER:
                    var grain = client.GetGrain<ICustomerAccountGroupGrain>(grainID);
                    if (isDet()) return grain.StartTransaction(grainAccessInfo, functionName, input);
                    else return grain.StartTransaction(functionName, input);
                case ImplementationType.ORLEANSEVENTUAL:
                    var eventuallyConsistentGrain = client.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(grainID);
                    return eventuallyConsistentGrain.StartTransaction(functionName, input);
                case ImplementationType.ORLEANSTXN:
                    var txnGrain = client.GetGrain<IOrleansTransactionalAccountGroupGrain>(grainID);
                    return txnGrain.StartTransaction(functionName, input);
                default:
                    return null;
            }
        }

        private int getAccountForGrain(int grainId)
        {
            return grainId * config.numAccountsPerGroup + accountIdDistribution.Sample();
        }

        public Task<TransactionResult> newTransaction(IClusterClient client, int eIndex)
        {
            var type = nextTransactionType();
            FunctionInput input = null;
            int groupId = 0;
            Dictionary<int, int> grainAccessInfo = null;
            if (type != TxnType.Transfer && type != TxnType.MultiTransfer)
            {
                if (config.distribution != Distribution.UNIFORM) throw new Exception("Exception: Only support UNIFORM for single grain txn. ");
                groupId = grainDistribution.Sample();
                var accountId = getAccountForGrain(groupId);
                switch (type)
                {
                    case TxnType.Balance:
                        input = new FunctionInput(accountId.ToString());
                        break;
                    case TxnType.DepositChecking:
                        var args1 = new Tuple<Tuple<string, int>, float>(new Tuple<string, int>(accountId.ToString(), accountId), transferAmountDistribution.Sample());
                        input = new FunctionInput(args1);
                        break;
                    case TxnType.TransactSaving:
                        var args2 = new Tuple<string, float>(accountId.ToString(), transferAmountDistribution.Sample());
                        input = new FunctionInput(args2);
                        break;
                    case TxnType.WriteCheck:
                        var args3 = new Tuple<string, float>(accountId.ToString(), transferAmountDistribution.Sample());
                        input = new FunctionInput(args3);
                        break;
                    default:
                        break;
                }
                grainAccessInfo = new Dictionary<int, int>();
                grainAccessInfo.Add(groupId, 1);
            }
            else if (type == TxnType.Transfer)
            {
                int destinationId;
                switch (config.distribution)
                {
                    case Distribution.UNIFORM:
                        groupId = grainDistribution.Sample();
                        do destinationId = grainDistribution.Sample();
                        while (groupId == destinationId);    // find a group which is not source group
                        break;
                    case Distribution.ZIPFIAN:
                        groupId = grainIDs[eIndex][index++];
                        do destinationId = grainIDs[eIndex][index++];
                        while (groupId == destinationId);    // find a group which is not source group
                        break;
                    case Distribution.HOTRECORD:
                        groupId = hotGrainDistribution.Sample();
                        destinationId = normalGrainDistribution.Sample();
                        break;
                    default:
                        throw new Exception("Exception: Unknown distribution. ");
                }
                var sourceAccountId = getAccountForGrain(groupId);
                var item1 = new Tuple<string, int>(sourceAccountId.ToString(), sourceAccountId);
                var destinationAccountId = getAccountForGrain(destinationId);
                var item2 = new Tuple<string, int>(destinationAccountId.ToString(), destinationAccountId);
                var item3 = transferAmountDistribution.Sample();
                var args = new Tuple<Tuple<string, int>, Tuple<string, int>, float>(item1, item2, item3);
                input = new FunctionInput(args);
                grainAccessInfo = new Dictionary<int, int>();
                grainAccessInfo.Add(groupId, 1);
                grainAccessInfo.Add(destinationId, 1);
            }
            else
            {
                var accountGrains = new HashSet<int>();
                switch (config.distribution)
                {
                    case Distribution.UNIFORM:
                        do accountGrains.Add(grainDistribution.Sample());
                        while (accountGrains.Count != config.numGrainsMultiTransfer);
                        break;
                    case Distribution.ZIPFIAN:
                        do accountGrains.Add(grainIDs[eIndex][index++]);
                        while (accountGrains.Count != config.numGrainsMultiTransfer);
                        break;
                    case Distribution.HOTRECORD:
                        var hotGrain = (int)(config.numGrainsMultiTransfer * hotGrainRatio);
                        do accountGrains.Add(hotGrainDistribution.Sample());    // add grainID from hot set
                        while (accountGrains.Count != hotGrain);
                        do accountGrains.Add(normalGrainDistribution.Sample());
                        while (accountGrains.Count != config.numGrainsMultiTransfer);
                        break;
                    default:
                        throw new Exception("Exception: Unknown distribution. ");
                }
                grainAccessInfo = new Dictionary<int, int>();
                Tuple<string, int> item1 = null;
                var item2 = transferAmountDistribution.Sample();
                var item3 = new List<Tuple<string, int>>();
                var first = true;
                foreach (var item in accountGrains)
                {
                    if (first)
                    {
                        first = false;
                        groupId = item;
                        grainAccessInfo.Add(groupId, 1);
                        var sourceId = getAccountForGrain(item);
                        item1 = new Tuple<string, int>(sourceId.ToString(), sourceId);
                        continue;
                    }
                    var destAccountId = getAccountForGrain(item);
                    item3.Add(new Tuple<string, int>(destAccountId.ToString(), destAccountId));
                    grainAccessInfo.Add(item, 1);
                }
                var args = new Tuple<Tuple<string, int>, float, List<Tuple<string, int>>>(item1, item2, item3);
                input = new FunctionInput(args);
            }
            var task = Execute(client, groupId, type.ToString(), input, grainAccessInfo);
            return task;
        }
    }
}