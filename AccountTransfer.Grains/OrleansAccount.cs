using AccountTransfer.Interfaces;
using Orleans;
using Orleans.CodeGeneration;
using Orleans.Transactions.Abstractions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

[assembly: GenerateSerializer(typeof(AccountTransfer.Grains.Balance))]

namespace AccountTransfer.Grains

{

    [Serializable]
    public class OrleansBalance

    {
        public uint Value { get; set; } = 1000;
        public uint Count { get; set; } = 0;

    }



    public class OrleansAccount : Grain, IOrleansAccount

    {

        private readonly ITransactionalState<OrleansBalance> balance;



        public OrleansAccount([TransactionalState("balance")] ITransactionalState<OrleansBalance> balance)

        { 
            this.balance = balance ?? throw new ArgumentNullException(nameof(balance));
        }



        Task IOrleansAccount.Deposit(uint amount)

        {
            try
            {
                this.balance.State.Value += amount;
                this.balance.State.Count++;
                this.balance.Save();
            }
            catch(Exception e)
            {
                throw e;
            }

            return Task.CompletedTask;

        }



        Task IOrleansAccount.Withdraw(uint amount)

        {
            try
            {
                this.balance.State.Value -= amount;
                this.balance.State.Count++;
                this.balance.Save();
            }
            catch (Exception e)
            {
                throw e;
            }



            return Task.CompletedTask;

        }



        Task<uint> IOrleansAccount.GetBalance()

        {

            return Task.FromResult(this.balance.State.Value);

        }
        Task<uint> IOrleansAccount.GetCount()

        {

            return Task.FromResult(this.balance.State.Count);

        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task ActivateGrain()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            return;
        }

    }

}