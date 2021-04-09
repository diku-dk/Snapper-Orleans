using System;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;

namespace TPCC.Grains
{
    public class EventualWarehouseGrain : Orleans.Grain, IEventualWarehouseGrain
    {
        WarehouseInfo state = new WarehouseInfo();

        public Task<TransactionResult> StartTransaction(string startFunction, FunctionInput inputs)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunction.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunction}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(inputs);
                case AllTxnTypes.GetWTax:
                    return GetWTax(inputs);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input: int     W_ID
        // output: null
        private async Task<TransactionResult> Init(FunctionInput fin)
        {
            var res = new TransactionResult();
            try
            {
                var W_ID = (int)fin.inputObject;   // W_ID
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
        private async Task<TransactionResult> GetWTax(FunctionInput fin)
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