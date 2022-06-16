using System;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;
using Orleans.Transactions;

namespace TPCC.Grains
{
    public class EventualWarehouseGrain : Orleans.Grain, IEventualWarehouseGrain
    {
        WarehouseInfo state = new WarehouseInfo();

        public Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(funcInput);
                case AllTxnTypes.GetWTax:
                    return GetWTax(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input: int     W_ID
        // output: null
        private async Task<TransactionResult> Init(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var W_ID = (int)funcInput;   // W_ID
                var myState = state;
                myState.warehouse = InMemoryDataGenerator.GenerateWarehouseInfo(W_ID);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: null
        // output: float    D_TAX
        private async Task<TransactionResult> GetWTax(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = state;
                res.resultObject = myState.warehouse.W_TAX;
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}