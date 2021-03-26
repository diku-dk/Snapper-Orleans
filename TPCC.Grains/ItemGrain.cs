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
    public class ItemTable : ICloneable
    {
        public Dictionary<int, Item> items;  // key: I_ID

        public ItemTable()
        {
            items = new Dictionary<int, Item>();
        }

        public ItemTable(ItemTable warehouse)
        {
            items = warehouse.items;
        }

        object ICloneable.Clone()
        {
            return new ItemTable(this);
        }
    }

    public class ItemGrain : TransactionExecutionGrain<ItemTable>, IItemGrain
    {
        public ItemGrain(IPersistSingletonGroup persistSingletonGroup) : base(persistSingletonGroup, "TPCC.Grains.ItemGrain")
        {
        }

        // input, output: null
        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            var context = fin.context;
            var res = new FunctionResult();
            res.isReadOnlyOnGrain = true;     // Yijian: avoid logging, just for run experiemnt easier
            try
            {
                var myState = await state.ReadWrite(context);
                myState.items = InMemoryDataGenerator.GenerateItemTable();
            }
            catch (Exception e)
            {
                res.setException();
            }
            return res;
        }

        // input: List<int> (item IDs)
        // output: Dictionary<int, float> (I_ID, item price)
        public async Task<FunctionResult> GetItemsPrice(FunctionInput fin)
        {
            var context = fin.context;
            var res = new FunctionResult();
            res.isReadOnlyOnGrain = true;
            try
            {
                var item_ids = (List<int>)fin.inputObject;
                var item_prices = new Dictionary<int, float>();  // <I_ID, price>
                var myState = await state.Read(context);
                foreach (var id in item_ids)
                {
                    if (myState.items.ContainsKey(id)) item_prices.Add(id, myState.items[id].I_PRICE);
                    else throw new Exception("Exception: invalid I_ID");
                }
                res.resultObject = item_prices;
            }
            catch (Exception e)
            {
                res.setException();
            }
            return res;
        }
    }
}
