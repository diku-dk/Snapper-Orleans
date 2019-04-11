using AccountTransfer.Interfaces;
using Orleans;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace ExperimentProcess
{
    class SmallBankBenchmark : IBenchmark
    {
        Random rand = new Random();
        int maxAccounts = 10;
        int maxTransferAmount = 10;
        int numSequentialTransfers = 10;
        int numConcurrentTransfers = 1000;
        List<Tuple<uint, uint, float, bool>> transactions;

        public void GenerateBenchmark(WorkloadConfiguration config)
        {
            //number of transaction
            int N = config.totalTransactions / (config.numThreadsPerWorkerNodes * config.numWorkerNodes);
            transactions = GenerateTransferInformation(N, new Tuple<int, int>(1, maxAccounts / 2), new Tuple<int, int>((maxAccounts / 2) + 1, maxAccounts), new Tuple<int, int>(1, maxTransferAmount), new Tuple<bool, bool>(false, true));
        }

        public async Task<WorkloadResults> RunBenchmark(IClusterClient client)
        {

            List<Tuple<Boolean, double>> results = new List<Tuple<bool, double>>();
            Stopwatch globalStopwatch = new Stopwatch();
            globalStopwatch.Start();
            foreach (var transferInfoTuple in transactions)
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
                }
                else
                {
                    //Non-deterministic transaction
                    task = fromAccount.StartTransaction("Transfer", input);
                }
                await task;
            }
            globalStopwatch.Stop();
            long totaltime = globalStopwatch.ElapsedMilliseconds;


            return null;
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

    }
}
