using System;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace TPCC.Grains
{
    public class EventualItemGrain : Orleans.Grain, IEventualItemGrain
    {
        ItemTable state = new ItemTable();

        public Task<TransactionResult> StartTransaction(string startFunction, FunctionInput inputs)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunction.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunction}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(inputs);
                case AllTxnTypes.GetItemsPrice:
                    return GetItemsPrice(inputs);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input, output: null
        private async Task<TransactionResult> Init(FunctionInput fin)
        {
            var res = new TransactionResult();
            try
            {
                var myState = state;
                myState.items = InMemoryDataGenerator.GenerateItemTable();
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: List<int> (item IDs)
        // output: Dictionary<int, float> (I_ID, item price)
        private async Task<TransactionResult> GetItemsPrice(FunctionInput fin)
        {
            var res = new TransactionResult();
            try
            {
                var item_ids = (List<int>)fin.inputObject;
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