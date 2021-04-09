using System;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;

namespace TPCC.Grains
{
    // each DistrictGrain only represents one district in an warehouse
    public class EventualDistrictGrain : Orleans.Grain, IEventualDistrictGrain
    {
        DistrictInfo state = new DistrictInfo();

        public Task<TransactionResult> StartTransaction(string startFunction, FunctionInput inputs)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunction.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunction}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(inputs);
                case AllTxnTypes.GetDTax:
                    return GetDTax(inputs);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input: W_ID, D_ID
        // output: null
        private async Task<TransactionResult> Init(FunctionInput fin)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int>)fin.inputObject;   // W_ID, D_ID
                var myState = state;
                myState.district = InMemoryDataGenerator.GenerateDistrictInfo(input.Item2);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        // input: null
        // output: Tuple<float, long>    D_TAX, O_ID
        private async Task<TransactionResult> GetDTax(FunctionInput fin)
        {
            var res = new TransactionResult();
            try
            {
                var myState = state;
                var O_ID = myState.district.D_NEXT_O_ID++;
                res.resultObject = new Tuple<float, long>(myState.district.D_TAX, O_ID);
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}