using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans;
using Concurrency.Interface;
using Concurrency.Utilities;

namespace TPCC.Interfaces
{
    public class StockItemUpdate
    {
        public UInt32 warehouseId;
        public UInt32 itemId;
        public UInt16 itemQuantity;
        public float price;
        public String districtInformation;

        public StockItemUpdate(uint warehouseId, uint itemId, ushort itemQuantity, float price, string districtInformation)
        {
            this.warehouseId = warehouseId;
            this.itemId = itemId;
            this.itemQuantity = itemQuantity;
            this.price = price;
            this.districtInformation = districtInformation;
        }
    }
    public class StockUpdateResult
    {
        public List<StockItemUpdate> stockItemUpdates;

        public StockUpdateResult()
        {
            stockItemUpdates = new List<StockItemUpdate>();
        }

        public StockUpdateResult(List<StockItemUpdate> stockItemUpdates)
        {
            this.stockItemUpdates = stockItemUpdates;
        }
    }

    public class PaymentInfo
    {
        public UInt32 warehouseId;
        public UInt32 districtId;
        public UInt32 customerWarehouseId;
        public UInt32 customerDistrictId;
        public UInt32 customerId;
        public String customerLastName;
        public float paymentAmount;

        public PaymentInfo(uint warehouseId, uint districtId, uint customerWarehouseId, uint customerDistrictId, uint customerId, string customerLastName, float paymentAmount)
        {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.customerWarehouseId = customerWarehouseId;
            this.customerDistrictId = customerDistrictId;
            this.customerId = customerId;
            this.customerLastName = customerLastName;
            this.paymentAmount = paymentAmount;
        }
    }

    public class NewOrderInput
    {
        public UInt32 warehouseId;
        public UInt32 districtId;
        public UInt32 customerId;
        public Dictionary<UInt32, Dictionary<UInt32, UInt16>> ordersPerWarehousePerItem;

        public NewOrderInput(uint warehouseId, uint districtId, uint customerId, Dictionary<uint, Dictionary<uint, ushort>> ordersPerWarehousePerItem)
        {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.customerId = customerId;
            this.ordersPerWarehousePerItem = ordersPerWarehousePerItem;
        }
    }

    public class StockUpdateInput
    {
        public UInt32 warehouseId;
        public UInt32 districtId;
        public Dictionary<UInt32, UInt16> ordersPerItem;

        public StockUpdateInput(uint warehouseId, uint districtId, Dictionary<uint, ushort> ordersPerItem)
        {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.ordersPerItem = ordersPerItem;
        }
    }

    public class FindCustomerIdInput
    {
        public UInt32 districtId;
        public String customerLastName;

        public FindCustomerIdInput(uint districtId, String customerLastName)
        {
            this.districtId = districtId;
            this.customerLastName = customerLastName;
        }
    }

    public interface IWarehouseGrain : ITransactionExecutionGrain, IGrainWithIntegerKey
    {
        Task<FunctionResult> NewOrder(FunctionInput functionInput);

        Task<FunctionResult> StockUpdate(FunctionInput functionInput);

        Task<FunctionResult> Payment(FunctionInput functionInput);

        Task<FunctionResult> CustomerPayment(FunctionInput functionInput);

        Task<FunctionResult> FindCustomerId(FunctionInput functionInput);
    }
}
