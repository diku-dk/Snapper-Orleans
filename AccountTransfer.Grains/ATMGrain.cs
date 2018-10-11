using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using AccountTransfer.Interfaces;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Concurrency.Utilities;
using Concurrency.Implementation;
using Utilities;

namespace AccountTransfer.Grains
{

    public class ATMGrain : TransactionExecutionGrain<Balance>, IATMGrain
    {

        TaskCompletionSource<String> promise = new TaskCompletionSource<String>();
        private Dictionary<int, TaskCompletionSource<String>> promiseMap = new Dictionary<int, TaskCompletionSource<String>>();

        public ATMGrain()
        {
            this.myUserClassName = "AccountTransfer.Grains.ATMGrain";
        }
        public Task<string> getPromise()
        {
            return promise.Task;
        }

        public Task setpromise()
        {
            Console.WriteLine($"\n\n Before set promise \n\n");
            bool ret = promise.TrySetResult("hello");
            Console.WriteLine($"\n\n Set Promise in ATM ! + {ret} + { promise.Task.Result}+ {promise.Task.Status.ToString()}\n\n");
            return Task.CompletedTask;
        }

        public Task setpromise(int i)
        {
            if (promiseMap.ContainsKey(i))
                promiseMap[i].SetResult("hello");
            else
                Console.WriteLine($"\n\n Promise {i} doesn't exist. \n\n");
            return Task.CompletedTask;
        }

        public async Task<int> testReentrance(int i)
        {

            if (promiseMap.ContainsKey(i) == false)
            {
                Console.WriteLine($"\n\n Receive call: {i}. \n\n");
                Console.WriteLine($"\n\n Size before add: {promiseMap.Count}. \n\n");
                promiseMap.Add(i, new TaskCompletionSource<string>());
                Console.WriteLine($"\n\n Size after add: {promiseMap.Count}. \n\n");

            }

            if (i == 1)
            {
                Console.WriteLine($"\n\n Executed call: {i}. \n\n");
                promiseMap[2].SetResult("hello");
            }

            if (i == 2)
            {
                await promiseMap[i].Task;
                //await Task.Delay(TimeSpan.FromSeconds(10));
                Console.WriteLine($"\n\n Executed call: {i}. \n\n");
            }

            Console.WriteLine($"\n\n Return call: {i}. \n\n");
            return i;
        }

        public async Task<FunctionResult> Transfer(FunctionInput functionInput)
        {

            TransactionContext context = functionInput.context;
            var input = (TransferInput)functionInput.inputObject;

            IAccountGrain fromAccount = this.GrainFactory.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(input.sourceAccount));
            IAccountGrain toAccount = this.GrainFactory.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(input.destinationAccount));

            FunctionInput input_1 = new FunctionInput(functionInput, input.transferAmount);
            FunctionInput input_2 = new FunctionInput(functionInput, input.transferAmount);
            FunctionCall c1 = new FunctionCall(typeof(AccountGrain), "Withdraw", input_1);
            FunctionCall c2 = new FunctionCall(typeof(AccountGrain), "Deposit", input_2);
            //Console.WriteLine($"\n\n ATm transfer from : {input.sourceAccount} to {input.destinationAccount}. \n\n");

            Task<FunctionResult> t1 = fromAccount.Execute(c1);
            Task<FunctionResult> t2 = toAccount.Execute(c2);
            await Task.WhenAll(t1, t2);
            //await t1;
            FunctionResult ret = new FunctionResult();
            ret.mergeWithFunctionResult(t1.Result);
            ret.mergeWithFunctionResult(t2.Result);
            return ret;
        }

        public async Task<FunctionResult> TransferOneToMulti(FunctionInput functionInput)
        {
            TransactionContext context = functionInput.context;
            var input = (TransferOneToMultiInput)functionInput.inputObject;
            var resultTasks = new List<Task<FunctionResult>>();
            resultTasks.Add(this.GrainFactory.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(input.sourceAccount)).Execute(new FunctionCall(typeof(AccountGrain), "Withdraw", new FunctionInput(functionInput, input.transferAmount * input.destinationAccounts.Count))));
            foreach (var destinationAccount in input.destinationAccounts)
            {
                resultTasks.Add(this.GrainFactory.GetGrain<IAccountGrain>(Helper.convertUInt32ToGuid(destinationAccount)).Execute(new FunctionCall(typeof(AccountGrain), "Deposit", new FunctionInput(functionInput, input.transferAmount))));
            }
            var results = await Task.WhenAll(resultTasks);

            FunctionResult myResult = new FunctionResult();
            foreach (var result in results)
            {
                result.mergeWithFunctionResult(result);
            }
            return myResult;
        }

        public Task<int> ActivateGrain()
        {
            return Task.FromResult(1);
        }
    }
}
