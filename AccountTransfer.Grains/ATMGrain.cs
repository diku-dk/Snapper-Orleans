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

    public class ATMGrain : TransactionExecutionGrain<Balance>, IATMGrain
    {
        
        TaskCompletionSource<String> promise = new TaskCompletionSource<String>();
        private  Dictionary<int, TaskCompletionSource<String>> promiseMap = new Dictionary<int, TaskCompletionSource<String>>();

        public ATMGrain()
        {
            this.myUserClassName = "AccountTransfer.Grains.ATmGrain";
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

        public async Task<FunctionResult> Transfer(FunctionInput input)
        {

            TransactionContext context = input.context;
            List<object> inputs = input.inputObjects;

            IAccountGrain fromAccount = this.GrainFactory.GetGrain<IAccountGrain>((Guid) (inputs[0]));
            IAccountGrain toAccount = this.GrainFactory.GetGrain<IAccountGrain>((Guid)(inputs[1]));

            int amountToTransfer = (int)inputs[2];
            List<object> args = new List<object>();
            args.Add(amountToTransfer);

            FunctionInput input_1 = new FunctionInput(input, args);
            FunctionInput input_2 = new FunctionInput(input, args);
            FunctionCall c1 = new FunctionCall(typeof(AccountGrain), "Withdraw", input_1);
            FunctionCall c2 = new FunctionCall(typeof(AccountGrain), "Deposit", input_2);


            Task<FunctionResult> t1 = fromAccount.Execute(c1);
            Task<FunctionResult> t2 = toAccount.Execute(c2);
            await Task.WhenAll(t1, t2);

            return new FunctionResult(t1.Result, t2.Result);
        }

        public async Task<FunctionResult> TransferOneToMulti(FunctionInput input)
        {
            TransactionContext context = input.context;
            List<object> inputs = input.inputObjects;

            IAccountGrain fromAccount = this.GrainFactory.GetGrain<IAccountGrain>((Guid)(inputs[0]));
            int sum = (int)inputs[1];

            List<IAccountGrain> destinationAccountList = new List<IAccountGrain>();
            List<int> transferAmount = new List<int>();

            int numerOfDestination = (inputs.Count - 2) / 2;
            for(int i= 2; i< 2 + numerOfDestination; i++)
            {
                destinationAccountList.Add(this.GrainFactory.GetGrain<IAccountGrain>((Guid)(inputs[i])));
                transferAmount.Add((int)inputs[i + numerOfDestination]);
            }

            List<Task<FunctionResult>> taskList = new List<Task<FunctionResult>>();
            List<object> args = new List<object>{sum};
            taskList.Add(fromAccount.Execute(new FunctionCall(typeof(AccountGrain), "Withdraw", new FunctionInput(input, args))));
            for(int i=0; i< destinationAccountList.Count; i++)
            {
                args = new List<object> { transferAmount[i] };
                taskList.Add(destinationAccountList[i].Execute(new FunctionCall(typeof(AccountGrain), "Deposit", new FunctionInput(input, args))));
            }

            await Task.WhenAll(taskList);

            FunctionResult result = new FunctionResult();
            foreach(Task<FunctionResult> task in taskList)
            {
                result.mergeWithFunctionResult(task.Result);
            }


            return result;
        }
    }
}
