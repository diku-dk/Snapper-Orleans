using AccountTransfer.Interfaces;
using MathNet.Numerics.Distributions;
using Orleans;
using SmallBank.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace ExperimentProcess
{
    enum TxnType {Balance, DepositChecking, Transfer, TransactSaving, WriteCheck, MultiTransfer}
    class SmallBankBenchmark : IBenchmark
    {
        IDiscreteDistribution transactionTypeDistribution;
        WorkloadConfiguration config;
        IDiscreteDistribution distribution;
        IDiscreteDistribution grainDistribution;
        IClusterClient client;
        float amount = 1;

        public void generateBenchmark(WorkloadConfiguration workloadConfig, IClusterClient client)
        {
            config = workloadConfig;
            if (config.distribution == Utilities.Distribution.ZIPFIAN)
            {
                distribution = new Zipf(config.zipf, (int)config.numAccounts - 1, new Random());
                grainDistribution = new Zipf(config.zipf, (int)config.numAccounts / (int)config.numAccountsPerGroup - 1, new Random());
            }

            else if (config.distribution == Utilities.Distribution.UNIFORM)
            {
                distribution = new DiscreteUniform(0, (int)config.numAccounts - 1, new Random());
                grainDistribution = new DiscreteUniform(0, (int)config.numAccounts / (int)config.numAccountsPerGroup - 1, new Random());
            }
            transactionTypeDistribution = new DiscreteUniform(0, 99, new Random());
            this.client = client;
        }

        //getBalance, depositChecking, transder, transacSaving, writeCheck, multiTransfer
        public TxnType nextTransactionType()
        {
            int type = transactionTypeDistribution.Sample();
            if (type < config.mixture[0])
                return TxnType.Balance;
            else if (type < config.mixture[1])
                return TxnType.DepositChecking;
            else if (type < config.mixture[2])
                return TxnType.Transfer;
            else if (type < config.mixture[3])
                return TxnType.TransactSaving;
            else if (type < config.mixture[4])
                return TxnType.WriteCheck;
            else
                return TxnType.MultiTransfer;
        }

        public Task<FunctionResult> newTransaction()
        {
            TxnType type = nextTransactionType();
            Task<FunctionResult> task = null;
            FunctionInput input;
            if (type != TxnType.Transfer && type!= TxnType.MultiTransfer)
            {
                uint accountId = Convert.ToUInt32(distribution.Sample());
                uint groupId = accountId / config.numAccountsPerGroup;
                var destination = client.GetGrain<ICustomerAccountGroupGrain>(Helper.convertUInt32ToGuid(groupId));
                
                switch (type)
                {
                    case TxnType.Balance:
                        input = new FunctionInput(accountId.ToString());
                        task = destination.StartTransaction("Balance", input);
                        break;
                    case TxnType.DepositChecking:
                        Tuple<Tuple<String, UInt32>, float> args1 = new Tuple<Tuple<string, uint>, float>(new Tuple<string, uint>(accountId.ToString(), accountId), amount);
                        input = new FunctionInput(args1);
                        task = destination.StartTransaction("DepositChecking", input);
                        break;
                    case TxnType.TransactSaving:
                        Tuple<String, float> args2 = new Tuple<string, float>(accountId.ToString(), amount);
                        input = new FunctionInput(args2);
                        task = destination.StartTransaction("TransactSaving", input);
                        break;
                    case TxnType.WriteCheck:
                        Tuple<String, float> args3 = new Tuple<string, float>(accountId.ToString(), amount);
                        input = new FunctionInput(args3);
                        task = destination.StartTransaction("WriteCheck", input);
                        break;
                    default:
                        break;
                }
                
            }
            else if(type == TxnType.Transfer)
            {
                uint sourceID = Convert.ToUInt32(distribution.Sample());
                Tuple<String, UInt32> item1 = new Tuple<string, uint>(sourceID.ToString(), sourceID);
                uint destID = Convert.ToUInt32(distribution.Sample());
                while(destID == sourceID)
                    destID = Convert.ToUInt32(distribution.Sample());
                Tuple<String, UInt32> item2 = new Tuple<string, uint>(destID.ToString(), destID);
                float item3 = 10;
                Tuple<Tuple<String, UInt32>, Tuple<String, UInt32>, float> args = new Tuple<Tuple<string, uint>, Tuple<string, uint>, float>(item1, item2, item3);
                input = new FunctionInput(args);
                var groupId = Helper.convertUInt32ToGuid(sourceID / config.numAccountsPerGroup);
                var destination = client.GetGrain<ICustomerAccountGroupGrain>(groupId);
                task = destination.StartTransaction("Transfer", input);
            }
            else
            {

                int[] grainIDs = new int[config.numGrainsMultiTransfer];
                distribution.Samples(grainIDs);
                int index = 0;
                List<UInt32> accounts = new List<uint>();
                int num = config.numAccountsMultiTransfer / grainIDs.Length;
                int bound = config.numAccountsMultiTransfer % grainIDs.Length;
                for (uint i=0; i<grainIDs.Length; i++)
                {
                    int numAccounts;
                    uint baseAccountID = Convert.ToUInt32(grainIDs[i]) * config.numAccountsPerGroup;
                    if(i < bound)
                    {
                        numAccounts = num+1;
                    }
                    else
                    {
                        numAccounts = num;
                    }
                    for(uint j = 0; j < numAccounts; j++)
                    {
                        accounts.Add(baseAccountID + j);
                    }
                }
                uint sourceID = accounts[0];
                Tuple<String, UInt32> item1 = new Tuple<string, uint>(sourceID.ToString(), sourceID);
                float item2 = this.amount;
                List<Tuple<string, uint>> item3 = new List<Tuple<string, uint>>();
                for (int i = 1; i < accounts.Count; i++)
                {
                    uint destAccountID = accounts[i];
                    item3.Add(new Tuple<string, uint>(destAccountID.ToString(), destAccountID));
                }
                var args = new Tuple<Tuple<String, UInt32>, float, List<Tuple<String, UInt32>>>(item1, item2, item3);
                input = new FunctionInput(args);
                var groupId = Helper.convertUInt32ToGuid(sourceID / config.numAccountsPerGroup);
                var destination = client.GetGrain<ICustomerAccountGroupGrain>(groupId);
                task = destination.StartTransaction("MultiTransfer", input);
            }
            return task;
        }


    }
}
