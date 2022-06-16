using System;
using Newtonsoft.Json;
using Orleans.Transactions.Abstractions;

namespace OrleansSnapperSiloHost
{
    public class StateEntity
    {
        public string internalState { get; set; }
        public string TransactionId { get; set; }
        public string TransactionManager { get; set; }
        public DateTime TransactionTimestamp { get; set; }
        public string StateJson { get => internalState; set => SetStateInternal(value); }

        public static StateEntity Create<T>(PendingTransactionState<T> pendingState) where T : class, new()
        {
            var result = new StateEntity
            {
                TransactionId = pendingState.TransactionId,
                TransactionTimestamp = pendingState.TimeStamp,
                TransactionManager = JsonConvert.SerializeObject(pendingState.TransactionManager),
            };

            result.SetState(pendingState.State);
            return result;
        }

        public T GetState<T>()
        {
            return JsonConvert.DeserializeObject<T>(internalState);
        }

        public void SetState<T>(T state)
        {
            internalState = JsonConvert.SerializeObject(state);
        }

        private void SetStateInternal(string stringData)
        {
            internalState = stringData;
        }
    }
}