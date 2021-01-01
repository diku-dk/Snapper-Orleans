using System;
using Orleans;
using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace TPCC.Interfaces
{
    public class NewOrderInput
    {
        public int C_ID;
        public DateTime O_ENTRY_D;
        public Dictionary<int, Tuple<int, int>> ItemsToBuy;  // <I_ID, <supply_warehouse, quantity>>

        public NewOrderInput(int C_ID, DateTime O_ENTRY_D, Dictionary<int, Tuple<int, int>> ItemsToBuy)
        {
            this.C_ID = C_ID;
            this.ItemsToBuy = ItemsToBuy;
        }
    }

    public class StockUpdateInput
    {
        public int W_ID;
        public int D_ID;
        public List<Tuple<int, int>> itemsToBuy;   // <I_ID, I_QUANTITY>

        public StockUpdateInput(int W_ID, int D_ID, List<Tuple<int, int>> itemsToBuy)
        {
            this.W_ID = W_ID;
            this.D_ID = D_ID;
            this.itemsToBuy = itemsToBuy;
        }
    }

    public class StockUpdateResult
    {
        public List<Tuple<int, string>> items;   // <I_ID, D_info>

        public StockUpdateResult(List<Tuple<int, string>> items)
        {
            this.items = items;
        }
    }

    public interface IWarehouseGrain : ITransactionExecutionGrain, IGrainWithIntegerKey
    {
        Task<FunctionResult> NewOrder(FunctionInput functionInput);
        Task<FunctionResult> StockUpdate(FunctionInput functionInput);
    }
}
