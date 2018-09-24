using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using Concurrency.Interface;
using Concurrency.Interface.Nondeterministic;
using Concurrency.Utilities;
using System.Threading.Tasks;

namespace Concurrency.Implementation.Nondeterministic
{
    public class NondeterministicTransactionCoordinator : Grain, INondeterministicTransactionCoordinator
    {
        private int curTransactionID;
        public override Task OnActivateAsync()
        {
            curTransactionID = 0;
            return base.OnActivateAsync();
        }


        public Task<TransactionContext> NewTransaction()
        {
            int tid = this.curTransactionID++;
            TransactionContext context = new TransactionContext(tid);
            //Console.WriteLine($"Coordinator: received Transaction {tid}");
            return Task.FromResult(context);
        }
    }
}
