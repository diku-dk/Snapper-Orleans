using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AccountTransfer.Interfaces
{
    public interface IOrleansATM : IGrainWithIntegerKey

    { 
        [Transaction(TransactionOption.RequiresNew)]
        Task Transfer(int fromAccount, int toAccount, uint amountToTransfer);

        Task ActivateGrain();

    }
}
