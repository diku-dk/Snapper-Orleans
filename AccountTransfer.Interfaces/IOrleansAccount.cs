using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AccountTransfer.Interfaces
{
    public interface IOrleansAccount : IGrainWithIntegerKey

    {

        [Transaction(TransactionOption.Required)]
        Task Withdraw(uint amount);



        [Transaction(TransactionOption.Required)]
        Task Deposit(uint amount);



        [Transaction(TransactionOption.Required)]
        Task<uint> GetBalance();

        [Transaction(TransactionOption.Required)]
        Task<uint> GetCount();

        Task ActivateGrain();

    }
}
