using System;
using Utilities;
using TPCC.Interfaces;
using Persist.Interfaces;
using System.Threading.Tasks;
using Concurrency.Implementation;
using System.Collections.Generic;
using System.Diagnostics;

namespace TPCC.Grains
{
    [Serializable]
    public class UpdateStockInput
    {
        public int W_ID;
        public int D_ID;
        public bool isRemote;
        public Dictionary<int, int> itemsToBuy;   // <I_ID, I_QUANTITY>

        public UpdateStockInput(int W_ID, int D_ID, bool isRemote, Dictionary<int, int> itemsToBuy)
        {
            this.W_ID = W_ID;
            this.D_ID = D_ID;
            this.isRemote = isRemote;
            this.itemsToBuy = itemsToBuy;
        }
    }

    [Serializable]
    public class StockTable : ICloneable
    {
        public int W_ID;
        public Dictionary<int, Stock> stock;  // key: I_ID

        public StockTable()
        {
            stock = new Dictionary<int, Stock>();
        }

        public StockTable(StockTable stock_table)
        {
            W_ID = stock_table.W_ID;
            stock = stock_table.stock;
        }

        object ICloneable.Clone()
        {
            return new StockTable(this);
        }
    }

    public class StockGrain : TransactionExecutionGrain<StockTable>, IStockGrain
    {
        public StockGrain(IPersistSingletonGroup persistSingletonGroup) : base(persistSingletonGroup, "TPCC.Grains.StockGrain")
        {
        }

        // input: Tuple<int, int>     W_ID, StockGrain index within the warehouse
        // output: null
        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            var context = fin.context;
            var res = new FunctionResult();
            res.isReadOnlyOnGrain = true;     // Yijian: avoid logging, just for run experiemnt easier
            try
            {
                var input = (Tuple<int, int>)fin.inputObject;    // W_ID, StockGrain index within the warehouse
                var myState = await state.ReadWrite(context);
                myState.W_ID = input.Item1;
                myState.stock = InMemoryDataGenerator.GenerateStockTable(input.Item2);
                //var serializer = new MsgPackSerializer();
                //var data = serializer.serialize(myState.stock);
                //Console.WriteLine($"StockGrain: W_ID  ={input.Item1}, size = {data.Length}");
            }
            catch (Exception e)
            {
                res.setException();
            }
            return res;
        }

        // input: UpdateStockInput   W_ID, D_ID, isRemote, <I_ID, I_QUANTITY>
        // output: Dictionary<int, string>    <I_ID, S_DIST_xx info>
        public async Task<FunctionResult> UpdateStock(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            var result = new Dictionary<int, string>();
            try
            {
                var input = (UpdateStockInput)fin.inputObject;   // W_ID, D_ID, isRemote, <I_ID, I_QUANTITY>
                var W_ID = input.W_ID;
                var D_ID = input.D_ID;
                var remoteFlag = input.isRemote ? 1 : 0;
                var items = input.itemsToBuy;
                if (items.Count == 0) throw new Exception("Exception: no items to buy");
                var myState = await state.ReadWrite(context);
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
                ret.setResult(result);
            }
            catch (Exception e)
            {
                //Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
                ret.setException();
            }
            return ret;
        }
    }
}
