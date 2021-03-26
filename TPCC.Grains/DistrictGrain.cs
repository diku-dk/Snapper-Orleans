using System;
using Utilities;
using TPCC.Interfaces;
using Persist.Interfaces;
using System.Threading.Tasks;
using Concurrency.Implementation;

namespace TPCC.Grains
{
    [Serializable]
    public class DistrictInfo : ICloneable
    {
        public int W_ID;
        public District district;  // key: I_ID

        public DistrictInfo()
        {
        }

        public DistrictInfo(DistrictInfo district_info)
        {
            W_ID = district_info.W_ID;
            district = district_info.district;
        }

        object ICloneable.Clone()
        {
            return new DistrictInfo(this);
        }
    }

    // each DistrictGrain only represents one district in an warehouse
    public class DistrictGrain : TransactionExecutionGrain<DistrictInfo>, IDistrictGrain
    {
        public DistrictGrain(IPersistSingletonGroup persistSingletonGroup) : base(persistSingletonGroup, "TPCC.Grains.DistrictGrain")
        {
        }

        // input: W_ID, D_ID
        // output: null
        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            try
            {
                var input = (Tuple<int, int>)fin.inputObject;   // W_ID, D_ID
                var myState = await state.ReadWrite(context);
                myState.district = InMemoryDataGenerator.GenerateDistrictInfo(input.Item2);
            }
            catch (Exception e)
            {
                ret.setException();
            }
            return ret;
        }

        // input: null
        // output: Tuple<float, long>    D_TAX, O_ID
        public async Task<FunctionResult> GetDTax(FunctionInput fin)
        {
            var context = fin.context;
            var res = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var O_ID = myState.district.D_NEXT_O_ID++;
                res.resultObject = new Tuple<float, long>(myState.district.D_TAX, O_ID);
            }
            catch (Exception e)
            {
                //Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
                res.setException();
            }
            return res;
        }
    }
}