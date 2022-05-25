﻿using System;
using Utilities;
using TPCC.Interfaces;
using System.Threading.Tasks;
using Concurrency.Implementation.TransactionExecution;
using Concurrency.Interface.Logging;
using System.Runtime.Serialization;
using Concurrency.Interface.Coordinator;

namespace TPCC.Grains
{
    [Serializable]
    public class WarehouseInfo : ICloneable, ISerializable
    {
        public Warehouse warehouse;

        public WarehouseInfo()
        {
        }

        public WarehouseInfo(WarehouseInfo warehouse_info)
        {
            warehouse = warehouse_info.warehouse;
        }

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("warehouse", warehouse, typeof(Warehouse));
        }

        object ICloneable.Clone()
        {
            return new WarehouseInfo(this);
        }
    }

    public class WarehouseGrain : TransactionExecutionGrain<WarehouseInfo>, IWarehouseGrain
    {
        public WarehouseGrain(ILoggerGroup loggerGroup, ICoordMap coordMap) : base(loggerGroup, coordMap, "TPCC.Grains.WarehouseGrain")
        {
        }

        // input: int     W_ID
        // output: null
        public async Task<TransactionResult> Init(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var W_ID = (int)funcInput;   // W_ID
                var myState = await GetState(context, AccessMode.ReadWrite);
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
        public async Task<TransactionResult> GetWTax(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var myState = await GetState(context, AccessMode.Read);
                res.resultObj = myState.warehouse.W_TAX;
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}