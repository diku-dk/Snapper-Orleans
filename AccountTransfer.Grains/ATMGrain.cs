using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;
using AccountTransfer.Interfaces;
using System.Collections.Generic;
using System.Collections.Concurrent;
using Concurrency.Utilities;
using Concurrency.Implementation;

namespace AccountTransfer.Grains
{

    public class ATMGrain : TransactionExecutionGrain, IATMGrain
    {
        TransactionContext context;
        TaskCompletionSource<String> promise = new TaskCompletionSource<String>();
        private  Dictionary<int, TaskCompletionSource<String>> promiseMap = new Dictionary<int, TaskCompletionSource<String>>();

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
                
            if(i == 1)
            {
                Console.WriteLine($"\n\n Executed call: {i}. \n\n");
                promiseMap[2].SetResult("hello");
            }

            if(i == 2)
            {
                await promiseMap[i].Task;
                //await Task.Delay(TimeSpan.FromSeconds(10));
                Console.WriteLine($"\n\n Executed call: {i}. \n\n");
            }

            Console.WriteLine($"\n\n Return call: {i}. \n\n");
            return i;
        }

        public async Task<List<object>> Transfer(List<object> inputs)
        {

            TransactionContext context = (TransactionContext)inputs[0];

            IDTransactionGrain fromAccount = (IDTransactionGrain)inputs[1];
            IDTransactionGrain toAccount = (IDTransactionGrain)inputs[2];

            int amountToTransfer = (int)inputs[3];
            List<object> args = new List<object>();
            args.Add(amountToTransfer);

            FunctionCall c1 = new FunctionCall(context, typeof(AccountGrain), "Withdraw", args);
            FunctionCall c2 = new FunctionCall(context, typeof(AccountGrain), "Deposit", args);

            List<object> ret = new List<object>();
            Task<List<object>> t1 =  fromAccount.Execute(c1);
            Task<List<object>> t2 =  toAccount.Execute(c2);
            await Task.WhenAll(t1, t2);
            
            return ret;
        }

        
    }
}
