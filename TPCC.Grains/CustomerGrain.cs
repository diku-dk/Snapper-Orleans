using System;
using Utilities;
using TPCC.Interfaces;
using Persist.Interfaces;
using System.Diagnostics;
using System.Threading.Tasks;
using Concurrency.Implementation;
using System.Collections.Generic;

namespace TPCC.Grains
{
    [Serializable]
    public class NewOrderInput
    {
        public int C_ID;
        public Dictionary<int, Tuple<int, int>> ItemsToBuy;  // <I_ID, <supply_warehouse, quantity>>

        public NewOrderInput(int C_ID, Dictionary<int, Tuple<int, int>> ItemsToBuy)
        {
            this.C_ID = C_ID;
            this.ItemsToBuy = ItemsToBuy;
        }
    }

    [Serializable]
    public class CustomerData : ICloneable
    {
        public int W_ID;
        public int D_ID;
        public Dictionary<int, Customer> customer_table;  // key: I_ID

        public CustomerData()
        {
            customer_table = new Dictionary<int, Customer>();
        }

        public CustomerData(CustomerData customer_data)
        {
            W_ID = customer_data.W_ID;
            D_ID = customer_data.D_ID;
            customer_table = new Dictionary<int, Customer>(customer_data.customer_table);
        }

        object ICloneable.Clone()
        {
            return new CustomerData(this);
        }
    }

    public class CustomerGrain : TransactionExecutionGrain<CustomerData>, ICustomerGrain
    {
        public CustomerGrain(IPersistSingletonGroup persistSingletonGroup) : base(persistSingletonGroup, "TPCC.Grains.CustomerGrain")
        {
        }

        // input: Tuple<int, int>     W_ID, D_ID
        // output: null
        public async Task<TransactionResult> Init(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int>)funcInput;   // <W_ID, D_ID>
                var myState = await GetState(context, AccessMode.Read);
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

        public async Task<TransactionResult> NewOrder(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var abort = false;
                var all_local = true;
                var txn_input = (NewOrderInput)funcInput;
                var C_ID = txn_input.C_ID;
                var ItemsToBuy = txn_input.ItemsToBuy;
                var myState = await GetState(context, AccessMode.Read);

                // STEP 1: get item prices from ItemGrain
                Dictionary<int, float> itemPrices;
                {
                    var itemIDs = new List<int>();
                    foreach (var item in ItemsToBuy) itemIDs.Add(item.Key);
                    var itemGrainID = Helper.GetItemGrain(myState.W_ID);
                    var func_call = new FunctionCall("GetItemsPrice", itemIDs, typeof(ItemGrain));
                    var r = await CallGrain(context, itemGrainID, "IItemGrain", func_call);
                    if (r.exception)
                    {
                        if (!context.isDet) throw new Exception("Exception thrown from ItemGrain. ");
                        abort = true;
                        itemPrices = new Dictionary<int, float>();
                    }
                    else itemPrices = (Dictionary<int, float>)r.resultObject;
                }

                // STEP 2: get tax info from WarehouseGrain and DistrictGrain
                float W_TAX;
                float D_TAX;
                long O_ID;
                {
                    var tasks = new List<Task<TransactionResult>>();
                    {
                        var func_call = new FunctionCall("GetWTax", null, typeof(WarehouseGrain));
                        var warehouseGrainID = Helper.GetWarehouseGrain(myState.W_ID);
                        tasks.Add(CallGrain(context, warehouseGrainID, "IWarehouseGrain", func_call));
                    }
                    {
                        var func_call = new FunctionCall("GetDTax", null, typeof(DistrictGrain));
                        var districtGrainID = Helper.GetDistrictGrain(myState.W_ID, myState.D_ID);
                        tasks.Add(CallGrain(context, districtGrainID, "IDistrictGrain", func_call));
                    }
                    await Task.WhenAll(tasks);
                    if (tasks[0].Result.exception || tasks[1].Result.exception)
                    {
                        if (!context.isDet) throw new Exception("Exception thrown from WarehouseGrain or DistrictGrain. ");
                        abort = true;
                    }
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
                            if (!abort) stockToUpdate[stockGrain].Add(I_ID, item.Value.Item2);
                        }
                    }

                    var tasks = new List<Task<TransactionResult>>();
                    foreach (var grain in stockToUpdate)
                    {
                        var func_input = new UpdateStockInput(myState.W_ID, myState.D_ID, isRemote[grain.Key], grain.Value);
                        var func_call = new FunctionCall("UpdateStock", func_input, typeof(StockGrain));
                        tasks.Add(CallGrain(context, grain.Key, "IStockGrain", func_call));
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
                    if (res.exception)
                    {
                        if (!context.isDet) throw new Exception("Exception thrown from StockGrain. ");
                        abort = true;
                    }
                }

                // STEP 4: insert records to OrderGrains
                {
                    var orderGrainID = Helper.GetOrderGrain(myState.W_ID, myState.D_ID, C_ID);
                    var orderGrain = GrainFactory.GetGrain<IOrderGrain>(orderGrainID);
                    if (abort)     // must finish the calls for PACT
                    {
                        Debug.Assert(context.isDet);
                        var func_call = new FunctionCall("AddNewOrder", null, typeof(OrderGrain));
                        _ = CallGrain(context, orderGrainID, "IOrderGrain", func_call);
                    }
                    else
                    {
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
                        var func_call = new FunctionCall("AddNewOrder", order_info, typeof(OrderGrain));
                        var t = CallGrain(context, orderGrainID, "IOrderGrain", func_call);
                        if (!context.isDet) await t;

                        var C_DISCOUNT = myState.customer_table[C_ID].C_DISCOUNT;
                        total_amount *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
                        res.resultObject = total_amount;
                    }
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