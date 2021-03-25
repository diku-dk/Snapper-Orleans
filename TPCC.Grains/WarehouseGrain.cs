using System;
using Utilities;
using TPCC.Interfaces;
using Persist.Interfaces;
using System.Threading.Tasks;
using Concurrency.Implementation;

namespace TPCC.Grains
{
    [Serializable]
    public class WarehouseInfo : ICloneable
    {
        public Warehouse warehouse;

        public WarehouseInfo()
        {
        }

        public WarehouseInfo(WarehouseInfo warehouse_info)
        {
            warehouse = warehouse_info.warehouse;
        }

        object ICloneable.Clone()
        {
            return new WarehouseInfo(this);
        }
    }

    public class WarehouseGrain : TransactionExecutionGrain<WarehouseInfo>, IWarehouseGrain
    {
        public WarehouseGrain(IPersistSingletonGroup persistSingletonGroup) : base(persistSingletonGroup, "TPCC.Grains.WarehouseGrain")
        {
        }

        // input: int     W_ID
        // output: null
        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            try
            {
                var W_ID = (int)fin.inputObject;   // W_ID
                var myState = await state.ReadWrite(context);
                myState.warehouse = InMemoryDataGenerator.GenerateWarehouseInfo(W_ID);
            }
            catch (Exception e)
            {
                ret.setException();
            }
            return ret;
        }

        // input: null
        // output: float    D_TAX
        public async Task<FunctionResult> GetWTax(FunctionInput fin)
        {
            var context = fin.context;
            var res = new FunctionResult();
            res.isReadOnlyOnGrain = true;
            try
            {
                var myState = await state.Read(context);
                res.resultObject = myState.warehouse.W_TAX;
            }
            catch (Exception e)
            {
                res.setException();
            }
            return res;
        }
    }
}
