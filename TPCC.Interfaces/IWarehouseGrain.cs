using Orleans;
using Utilities;
using Concurrency.Interface;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace TPCC.Interfaces
{
    public class StockItemUpdate
    {
        public uint warehouseId;
        public uint itemId;
        public ushort itemQuantity;
        public float price;
        public string districtInformation;

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
        public uint warehouseId;
        public uint districtId;
        public uint customerWarehouseId;
        public uint customerDistrictId;
        public uint customerId;
        public string customerLastName;
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
        public uint warehouseId;
        public uint districtId;
        public uint customerId;
        public Dictionary<uint, Dictionary<uint, ushort>> ordersPerWarehousePerItem;

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
        public uint warehouseId;
        public uint districtId;
        public Dictionary<uint, ushort> ordersPerItem;

        public StockUpdateInput(uint warehouseId, uint districtId, Dictionary<uint, ushort> ordersPerItem)
        {
            this.warehouseId = warehouseId;
            this.districtId = districtId;
            this.ordersPerItem = ordersPerItem;
        }
    }

    public class FindCustomerIdInput
    {
        public uint districtId;
        public string customerLastName;

        public FindCustomerIdInput(uint districtId, string customerLastName)
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
