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

        public Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(funcInput);
                case AllTxnTypes.GetDTax:
                    return GetDTax(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input: W_ID, D_ID
        // output: null
        private async Task<TransactionResult> Init(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int>)funcInput;   // W_ID, D_ID
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
        private async Task<TransactionResult> GetDTax(object funcInput)
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