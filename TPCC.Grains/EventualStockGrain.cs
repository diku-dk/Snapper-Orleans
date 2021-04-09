using System;
using Utilities;
using TPCC.Interfaces;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace TPCC.Grains
{
    public class EventualStockGrain : Orleans.Grain, IEventualStockGrain
    {
        StockTable state = new StockTable();

        public Task<TransactionResult> StartTransaction(string startFunction, FunctionInput inputs)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunction.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunction}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(inputs);
                case AllTxnTypes.UpdateStock:
                    return UpdateStock(inputs);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input: Tuple<int, int>     W_ID, StockGrain index within the warehouse
        // output: null
        private async Task<TransactionResult> Init(FunctionInput fin)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int>)fin.inputObject;    // W_ID, StockGrain index within the warehouse
                var myState = state;
                myState.W_ID = input.Item1;
                myState.stock = InMemoryDataGenerator.GenerateStockTable(input.Item2);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: UpdateStockInput   W_ID, D_ID, isRemote, <I_ID, I_QUANTITY>
        // output: Dictionary<int, string>    <I_ID, S_DIST_xx info>
        private async Task<TransactionResult> UpdateStock(FunctionInput fin)
        {
            var ret = new TransactionResult();
            var result = new Dictionary<int, string>();
            try
            {
                var input = (UpdateStockInput)fin.inputObject;   // W_ID, D_ID, isRemote, <I_ID, I_QUANTITY>
                var W_ID = input.W_ID;
                var D_ID = input.D_ID;
                var remoteFlag = input.isRemote ? 1 : 0;
                var items = input.itemsToBuy;
                if (items.Count == 0) throw new Exception("Exception: no items to buy");
                var myState = state;
                if (remoteFlag == 1) Debug.Assert(W_ID != myState.W_ID);
                else Debug.Assert(W_ID == myState.W_ID);
                foreach (var item in items)
                {
                    var I_ID = item.Key;
                    var quantity = item.Value;

                    Debug.Assert(myState.stock.ContainsKey(I_ID));
                    var the_stock = myState.stock[I_ID];
                    var S_QUANTITY = the_stock.S_QUANTITY;
                    if (S_QUANTITY - quantity >= 10) S_QUANTITY -= quantity;
                    else S_QUANTITY += 91 - quantity;

                    the_stock.S_YTD += quantity;
                    the_stock.S_ORDER_CNT++;
                    the_stock.S_REMOTE_CNT += remoteFlag;

                    var S_DIST = the_stock.S_DIST[D_ID];
                    result.Add(I_ID, S_DIST);
                }
                ret.resultObject = result;
            }
            catch (Exception)
            {
                ret.exception = true;
            }
            return ret;
        }
    }
}