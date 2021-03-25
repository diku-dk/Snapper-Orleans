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
        public District district;  // key: I_ID

        public DistrictInfo()
        {
        }

        public DistrictInfo(DistrictInfo district_info)
        {
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

        // input: int (D_ID)
        // output: null
        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            try
            {
                var D_ID = (int)fin.inputObject;
                var myState = await state.ReadWrite(context);
                myState.district = InMemoryDataGenerator.GenerateDistrictInfo(D_ID);
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
            var ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                ret.resultObject = new Tuple<float, long>(myState.district.D_TAX, myState.district.D_NEXT_O_ID++);
            }
            catch (Exception e)
            {
                ret.setException();
            }
            return ret;
        }
    }
}