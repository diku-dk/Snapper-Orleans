using System;
using Utilities;
using TPCC.Interfaces;
using Persist.Interfaces;
using System.Threading.Tasks;
using Concurrency.Implementation;
using System.Collections.Generic;

namespace TPCC.Grains
{
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
        public Dictionary<int, Stock> stock;  // key: I_ID

        public StockTable()
        {
            stock = new Dictionary<int, Stock>();
        }

        public StockTable(StockTable stock_table)
        {
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

        // input: int    partitionID
        // output: null
        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            try
            {
                var partitionID = (int)fin.inputObject;
                var myState = await state.ReadWrite(context);
                myState.stock = InMemoryDataGenerator.GenerateStockTable(Constants.NUM_StockGrain_PER_W, partitionID);
            }
            catch (Exception e)
            {
                ret.setException();
            }
            return ret;
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
                foreach (var item in items)
                {
                    var I_ID = item.Key;
                    var quantity = item.Value;

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
                ret.setException();
            }
            return ret;
        }
    }
}
