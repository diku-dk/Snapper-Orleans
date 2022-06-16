using System;
using Newtonsoft.Json;
using Orleans.Transactions.Abstractions;
using MessagePack;

namespace SnapperSiloHost
{
    [MessagePackObject]
    public class StateEntity
    {
        [Key(0)]
        public string internalState { get; set; }
        [Key(1)]
        public string TransactionId { get; set; }
        [Key(2)]
        public string TransactionManager { get; set; }
        [Key(3)]
        public DateTime TransactionTimestamp { get; set; }
        [Key(4)]
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