using System;
using Utilities;
using TPCC.Interfaces;
using System.Diagnostics;
using Persist.Interfaces;
using System.Threading.Tasks;
using Concurrency.Implementation;
using System.Collections.Generic;

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
        public async Task<TransactionResult> Init(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.ReadWrite);
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
        public async Task<TransactionResult> GetItemsPrice(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var item_ids = (List<int>)funcInput;
                var item_prices = new Dictionary<int, float>();  // <I_ID, price>
                var myState = await GetState(context, AccessMode.Read);
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