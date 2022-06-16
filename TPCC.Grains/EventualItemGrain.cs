using System;
using Utilities;
using TPCC.Interfaces;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Transactions;

namespace TPCC.Grains
{
    public class EventualItemGrain : Orleans.Grain, IEventualItemGrain
    {
        ItemTable state = new ItemTable();

        public Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(funcInput);
                case AllTxnTypes.GetItemsPrice:
                    return GetItemsPrice(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input, output: null
        private async Task<TransactionResult> Init(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = state;
                if (myState.items.Count == 0) myState.items = InMemoryDataGenerator.GenerateItemTable();
                else Debug.Assert(myState.items.Count == Constants.NUM_I);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: List<int> (item IDs)
        // output: Dictionary<int, float> (I_ID, item price)
        private async Task<TransactionResult> GetItemsPrice(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var item_ids = (List<int>)funcInput;
                var item_prices = new Dictionary<int, float>();  // <I_ID, price>
                var myState = state;
                foreach (var id in item_ids)
                {
                    if (myState.items.ContainsKey(id)) item_prices.Add(id, myState.items[id].I_PRICE);
                    else throw new Exception("Exception: invalid I_ID");
                }
                res.resultObject = item_prices;
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}