using Orleans;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using AccountTransfer.Interfaces;

namespace AccountTransfer.Grains
{
    [StatelessWorker]
    public class OrleansATM : Grain, IOrleansATM
    {

        async Task IOrleansATM.Transfer(int fromAccount, int toAccount, uint amountToTransfer)

        {
            try
            {
                await Task.WhenAll(
                    this.GrainFactory.GetGrain<IOrleansAccount>(fromAccount).Withdraw(amountToTransfer),
                    this.GrainFactory.GetGrain<IOrleansAccount>(toAccount).Deposit(amountToTransfer));
            }catch(Exception e)
            {
                //Console.WriteLine($"\n\n ATM {this.GetPrimaryKey()}, Accounts {fromAccount} {toAccount}: {e.Message}\n\n");
                throw e;
            }
            return; 

        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async Task ActivateGrain()
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            return;
        }

    }
}
