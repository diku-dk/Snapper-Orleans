using System;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;
using Orleans.CodeGeneration;
using System.Collections.Generic;
using Concurrency.Implementation;

[assembly: GenerateSerializer(typeof(TPCC.Grains.WarehouseData))]

namespace TPCC.Grains
{
    public class WarehouseGrain : TransactionExecutionGrain<WarehouseData>, IWarehouseGrain
    {
        public WarehouseGrain() : base("TPCC.Grains.WarehouseGrain")
        {
        }

        private int order_local_count = 0;

        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            var input = (Tuple<int, int>)fin.inputObject;
            try
            {
                var myState = await state.ReadWrite(context);
                InMemoryDataGenerator.GenerateData(input.Item1, input.Item2, myState);
                //InMemoryDataGenerator.GenerateSimpleData(input.Item1, input.Item2, myState);
                //Console.WriteLine($"Init W {input.Item1}, D {input.Item2}, w.stock.count = {myState.stock_table.Count}");
            }
            catch (Exception e)
            {
                //Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> NewOrder(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            var input = (NewOrderInput)fin.inputObject;
            try
            {
                // STEP 0: validate item id
                var abort = false;
                var myState = await state.ReadWrite(context);
                var items = myState.item_table;
                foreach (var item in input.ItemsToBuy)
                {
                    if (!items.ContainsKey(item.Key))
                    {
                        if (context.isDeterministic) abort = true;
                        else throw new Exception($"Exception: invalid I_ID {item.Key}");
                    }
                }

                // STEP 1: check where to buy the item (local / remote grain)
                var all_local = true;
                var W_ID = myState.warehouse_info.W_ID;
                var stockToUpdate = new Dictionary<int, List<Tuple<int, int>>>();  // <grainID, <I_ID, quantity>>
                foreach (var item in input.ItemsToBuy)    // <I_ID, <supply_warehouse, quantity>>
                {
                    if (W_ID != item.Value.Item1) all_local = false;
                    if (items.ContainsKey(item.Key))
                    {
                        var dest = Helper.GetGrainID(item.Value.Item1, item.Key, false);
                        if (!stockToUpdate.ContainsKey(dest)) stockToUpdate.Add(dest, new List<Tuple<int, int>>());
                        stockToUpdate[dest].Add(new Tuple<int, int>(item.Key, item.Value.Item2));
                    }
                }

                // STEP 2: update stock
                var tasks = new List<Task<FunctionResult>>();
                var D_ID = myState.district_info.D_ID;
                var myID = Helper.GetGrainID(W_ID, D_ID, true);
                foreach (var grain in stockToUpdate)
                {
                    var func_input = new FunctionInput(fin, new StockUpdateInput(W_ID, D_ID, grain.Value));
                    if (grain.Key == myID) tasks.Add(StockUpdate(func_input));
                    else
                    {
                        var fc = new FunctionCall(typeof(WarehouseGrain), "StockUpdate", func_input);
                        var dest = GrainFactory.GetGrain<IWarehouseGrain>(grain.Key);
                        tasks.Add(dest.Execute(fc));
                    }
                }

                // STEP 3: insert some records, compute total_amount
                if (!abort)
                {
                    await Task.WhenAll(tasks);
                    var hasExp = false;
                    if (!context.isDeterministic)
                    {
                        foreach (var t in tasks)
                        {
                            ret.mergeWithFunctionResult(t.Result);
                            if (t.Result.hasException()) hasExp = true;
                        }
                    }

                    if (!hasExp)
                    {
                        myState.district_info.D_NEXT_O_ID++;
                        var O_ID = order_local_count++;
                        var neworder = new NewOrder(O_ID);
                        var order = new Order(O_ID, input.C_ID, input.O_ENTRY_D, null, input.ItemsToBuy.Count, all_local);
                        myState.neworder.Add(neworder);
                        myState.order_table.Add(O_ID, order);

                        var item_count = 0;
                        float total_amount = 0;
                        foreach (var t in tasks)
                        {
                            if (!context.isDeterministic) ret.mergeWithFunctionResult(t.Result);

                            var item_info = ((StockUpdateResult)t.Result.resultObject).items;   // <I_ID, price, D_info>
                            foreach (var i in item_info)
                            {
                                var I_ID = i.Item1;
                                var D_INFO = i.Item2;
                                var I_PRICE = items[I_ID].I_PRICE;
                                var QUANTITY = input.ItemsToBuy[I_ID].Item2;
                                var SUPPLY_W_ID = input.ItemsToBuy[I_ID].Item1;
                                var AMOUNT = I_PRICE * QUANTITY;
                                total_amount += AMOUNT;
                                var num = item_count++;
                                var orderline = new OrderLine(O_ID, num, I_ID, SUPPLY_W_ID, null, QUANTITY, AMOUNT, D_INFO);
                                myState.orderline_table.Add(new Tuple<int, int>(O_ID, num), orderline);
                            }
                        }
                        var C_DISCOUNT = myState.customer_table[input.C_ID].C_DISCOUNT;
                        var W_TAX = myState.warehouse_info.W_TAX;
                        var D_TAX = myState.district_info.D_TAX;
                        total_amount *= (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX);
                        ret.setResult(total_amount);
                    }
                }
            }
            catch (Exception e)
            {
                //Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
                if (!e.Message.Contains("I_ID")) ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> StockUpdate(FunctionInput fin)
        {
            var remoteFlag = 0;
            var context = fin.context;
            var ret = new FunctionResult();
            var input = (StockUpdateInput)fin.inputObject;
            var result = new List<Tuple<int, string>>();
            try
            {
                if (input.itemsToBuy.Count == 0) throw new Exception("Exception: empty item set");
                var myState = await state.ReadWrite(context);
                var W_ID = myState.warehouse_info.W_ID;
                var D_ID = myState.district_info.D_ID;
                remoteFlag = W_ID == input.W_ID ? 0 : 1;
                foreach (var item in input.itemsToBuy)   // <I_ID, quantity>
                {
                    var I_ID = item.Item1;
                    var quantity = item.Item2;

                    var the_stock = myState.stock_table[I_ID];
                    var S_QUANTITY = the_stock.S_QUANTITY;
                    if (S_QUANTITY - quantity >= 10) S_QUANTITY -= quantity;
                    else S_QUANTITY += 91 - quantity;

                    the_stock.S_YTD += quantity;
                    the_stock.S_ORDER_CNT++;
                    the_stock.S_REMOTE_CNT += remoteFlag;

                    var S_DIST = the_stock.S_DIST[D_ID];
                    result.Add(new Tuple<int, string>(I_ID, S_DIST));
                }
                ret.setResult(new StockUpdateResult(result));
            }
            catch (Exception e)
            {
                //Console.WriteLine($"Exception: remote = {remoteFlag == 1}, {e.Message}, {e.StackTrace}");
                ret.setException();
            }
            return ret;
        }
    }
}
