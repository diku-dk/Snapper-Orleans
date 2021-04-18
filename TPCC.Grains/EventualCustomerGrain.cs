using System;
using Utilities;
using TPCC.Interfaces;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace TPCC.Grains
{
    public class EventualCustomerGrain : Orleans.Grain, IEventualCustomerGrain
    {
        CustomerData state = new CustomerData();

        public Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(funcInput);
                case AllTxnTypes.NewOrder:
                    return NewOrder(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input: Tuple<int, int>     W_ID, D_ID
        // output: null
        private async Task<TransactionResult> Init(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int>)funcInput;   // <W_ID, D_ID>
                var myState = state;
                myState.W_ID = input.Item1;
                myState.D_ID = input.Item2;
                if (myState.customer_table.Count == 0) myState.customer_table = InMemoryDataGenerator.GenerateCustomerTable();
                else Debug.Assert(myState.customer_table.Count == Constants.NUM_C_PER_D);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        private async Task<TransactionResult> NewOrder(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var all_local = true;
                var txn_input = (NewOrderInput)funcInput;
                var C_ID = txn_input.C_ID;
                var ItemsToBuy = txn_input.ItemsToBuy;
                var myState = state;
                // STEP 1: get item prices from ItemGrain
                Dictionary<int, float> itemPrices;
                {
                    var itemIDs = new List<int>();
                    foreach (var item in ItemsToBuy) itemIDs.Add(item.Key);
                    var itemGrainID = Helper.GetItemGrain(myState.W_ID);
                    var itemGrain = GrainFactory.GetGrain<IEventualItemGrain>(itemGrainID);
                    var r = await itemGrain.StartTransaction("GetItemsPrice", itemIDs);
                    if (r.exception) throw new Exception("Exception thrown from ItemGrain. ");
                    itemPrices = (Dictionary<int, float>)r.resultObject;
                }

                // STEP 2: get tax info from WarehouseGrain and DistrictGrain
                float W_TAX;
                float D_TAX;
                long O_ID;
                {
                    var tasks = new List<Task<TransactionResult>>();
                    {
                        var warehouseGrainID = Helper.GetWarehouseGrain(myState.W_ID);
                        var warehouseGrain = GrainFactory.GetGrain<IEventualWarehouseGrain>(warehouseGrainID);
                        tasks.Add(warehouseGrain.StartTransaction("GetWTax", null));
                    }
                    {
                        var districtGrainID = Helper.GetDistrictGrain(myState.W_ID, myState.D_ID);
                        var districtGrain = GrainFactory.GetGrain<IEventualDistrictGrain>(districtGrainID);
                        tasks.Add(districtGrain.StartTransaction("GetDTax", null));
                    }
                    await Task.WhenAll(tasks);
                    if (tasks[0].Result.exception || tasks[1].Result.exception) throw new Exception("Exception thrown from WarehouseGrain or DistrictGrain. ");
                    W_TAX = (float)tasks[0].Result.resultObject;
                    var r = (Tuple<float, long>)tasks[1].Result.resultObject;
                    D_TAX = r.Item1;
                    O_ID = r.Item2;
                }

                // STEP 3: update stock in corresponding StockGrains
                var items_dist_info = new Dictionary<int, string>();
                {
                    var isRemote = new Dictionary<int, bool>();                       // <grainID, is remote warehouse>
                    var stockToUpdate = new Dictionary<int, Dictionary<int, int>>();  // <grainID, <I_ID, quantity>>
                    foreach (var item in ItemsToBuy)                                  // <I_ID, <supply W_ID, quantity>>
                    {
                        var I_ID = item.Key;
                        if (I_ID != -1)
                        {
                            var supply_W_ID = item.Value.Item1;
                            var stockGrain = Helper.GetStockGrain(supply_W_ID, I_ID);
                            if (!isRemote.ContainsKey(stockGrain))
                            {
                                if (myState.W_ID != supply_W_ID)
                                {
                                    all_local = false;
                                    isRemote.Add(stockGrain, true);
                                }
                                else isRemote.Add(stockGrain, false);
                                stockToUpdate.Add(stockGrain, new Dictionary<int, int>());
                            }
                            stockToUpdate[stockGrain].Add(I_ID, item.Value.Item2);
                        }
                    }

                    var tasks = new List<Task<TransactionResult>>();
                    foreach (var grain in stockToUpdate)
                    {
                        var func_input = new UpdateStockInput(myState.W_ID, myState.D_ID, isRemote[grain.Key], grain.Value);
                        var stockGrain = GrainFactory.GetGrain<IEventualStockGrain>(grain.Key);
                        tasks.Add(stockGrain.StartTransaction("UpdateStock", func_input));
                    }
                    await Task.WhenAll(tasks);
                    foreach (var t in tasks)
                    {
                        if (t.Result.resultObject != null)
                        {
                            var r = (Dictionary<int, string>)t.Result.resultObject;
                            foreach (var item in r) items_dist_info.Add(item.Key, item.Value);
                        }
                    }
                    if (res.exception) throw new Exception("Exception thrown from StockGrain. ");
                }

                // STEP 4: insert records to OrderGrains
                {
                    var orderGrainID = Helper.GetOrderGrain(myState.W_ID, myState.D_ID, C_ID);
                    var orderGrain = GrainFactory.GetGrain<IEventualOrderGrain>(orderGrainID);
                    var order = new Order(O_ID, C_ID, DateTime.Now.Date, null, ItemsToBuy.Count, all_local);
                    var item_count = 0;
                    float total_amount = 0;
                    var orderlines = new List<OrderLine>();
                    foreach (var item in ItemsToBuy)
                    {
                        var I_ID = item.Key;
                        var D_INFO = items_dist_info[I_ID];
                        var I_PRICE = itemPrices[I_ID];
                        var QUANTITY = ItemsToBuy[I_ID].Item2;
                        var SUPPLY_W_ID = ItemsToBuy[I_ID].Item1;
                        var AMOUNT = I_PRICE * QUANTITY;
                        total_amount += AMOUNT;
                        var num = item_count++;
                        var orderline = new OrderLine(O_ID, num, I_ID, SUPPLY_W_ID, null, QUANTITY, AMOUNT, D_INFO);
                        orderlines.Add(orderline);
                    }
                    var order_info = new OrderInfo(order, orderlines);
                    var t = orderGrain.StartTransaction("AddNewOrder", order_info);
                    await t;

                    var C_DISCOUNT = myState.customer_table[C_ID].C_DISCOUNT;
                    total_amount *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
                    res.resultObject = total_amount;
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}