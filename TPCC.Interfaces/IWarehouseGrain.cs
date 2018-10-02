using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans;
using Concurrency.Interface;

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

    public interface IWarehouseGrain : ITransactionExecutionGrain, IGrainWithIntegerKey
    {
        /*Task<float> NewOrder(UInt32 wareHouseId, UInt32 districtId, UInt32 customerId, Dictionary<UInt32, Dictionary<UInt32, UInt16>> ordersPerWarehousePerItem);

        Task<StockUpdateResult> StockUpdate(UInt32 warehouseId, UInt32 districtId, Dictionary<UInt32, UInt16> ordersPerItem);

        Task<float> Payment(PaymentInfo paymentInformation);

        Task<UInt32> CustomerPayment(PaymentInfo paymentInformation);

        Task<UInt32> FindCustomerId(UInt32 districtId, String customerLastName);        
        */
    }
}
