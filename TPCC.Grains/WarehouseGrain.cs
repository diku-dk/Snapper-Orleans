using System;
using TPCC.Interfaces;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.CodeGeneration;
using Concurrency.Implementation;

[assembly: GenerateSerializer(typeof(TPCC.Grains.WarehouseData))]

namespace TPCC.Grains
{
    
    public class WarehouseGrain : TransactionExecutionGrain<WarehouseData>, IWarehouseGrain
    {        

       /* async Task<float> IWarehouseGrain.NewOrder(UInt32 warehouseId, UInt32 districtId, UInt32 customerId, Dictionary<UInt32, Dictionary<UInt32, UInt16>> ordersPerWarehousePerItem)
        {    

            int orderLineCount = 0; bool allLocal = true;
            var stockUpdates = new List<Task<StockUpdateResult>>();
            foreach(var orderEntry in ordersPerWarehousePerItem)
            {
                orderLineCount += orderEntry.Value.Count;
                if(orderEntry.Key != warehouseId)
                {
                    allLocal = false;                    
                }
                stockUpdates.Add(Task.Run(() => this.GrainFactory.GetGrain<IWarehouseGrain>(warehouseId).StockUpdate(warehouseId, districtId, orderEntry.Value)));                    
            }
            var state = 
            //Get customer information
            var customerKey = new Tuple<UInt32, UInt32>(districtId, customerId);
            var customer = state.customerRecords[customerKey];

            //Get district information
            var district = state.districtRecords[districtId];
            var districtNextOrderId = district.nextOrderId;            
            district.nextOrderId++;

            //Create entry in new order
            state.newOrders.Add(new NewOrder(districtId, districtNextOrderId));

            //Create entry in order
            var orderKey = new Tuple<UInt32, UInt32>(districtId, districtNextOrderId);
            state.orderRecords.Add(orderKey, new Order(customerId, 0, (UInt16) orderLineCount, allLocal, 0));

            //Consume the tasks as they arrive, add orderlines and compute total
            float totalAmount = 0;
            while (stockUpdates.Count != 0)
            {
                var stockUpdateResultTask = await Task.WhenAny(stockUpdates);
                var stockUpdateResult = await stockUpdateResultTask;
                //Add order line entries
                UInt16 orderLineCounter = 1;
                foreach (var aStockItemUpdateResult in stockUpdateResult.stockItemUpdates) 
                {
                    var orderLineKey = new Tuple<UInt32, UInt32, UInt16>(districtId, districtNextOrderId, orderLineCounter++);
                    state.orderLineRecords.Add(orderLineKey, new OrderLine(aStockItemUpdateResult.itemId, 0, aStockItemUpdateResult.price, aStockItemUpdateResult.warehouseId, aStockItemUpdateResult.itemQuantity, aStockItemUpdateResult.districtInformation));
                    totalAmount += aStockItemUpdateResult.price;                            
                }
                stockUpdates.Remove(stockUpdateResultTask);
            }

            //Compute total
            totalAmount *= (1 + state.warehouseRecord.tax + district.tax) * (1 - customer.discount);
            return totalAmount;
        }

        Task<StockUpdateResult> IWarehouseGrain.StockUpdate(UInt32 warehouseId, UInt32 districtId, Dictionary<UInt32, UInt16> ordersPerItem)
        {
            var state = state.read
            var result = new StockUpdateResult();
            foreach (var itemOrdered in ordersPerItem)
            {
                var item = state.itemRecords[itemOrdered.Key];
                var stock = state.stockRecords[itemOrdered.Key];
                //Stock quantity update with auto replenishment
                if(stock.quantity - itemOrdered.Value >= 10)
                {
                    stock.quantity -= itemOrdered.Value;
                } else
                {
                    stock.quantity = (UInt16) (itemOrdered.Value + 91);
                }
                stock.ytd += itemOrdered.Value;
                if(warehouseId == state.warehouseRecord.wareHouseId)
                {
                    stock.remoteCount++;
                }

                //Construct district information for the result
                var districtInfo = default(String);
                switch(districtId)
                {
                    case 1:
                        districtInfo = stock.dist01;
                        break;
                    case 2:
                        districtInfo = stock.dist02;
                        break;
                    case 3:
                        districtInfo = stock.dist03;
                        break;
                    case 4:
                        districtInfo = stock.dist04;
                        break;
                    case 5:
                        districtInfo = stock.dist05;
                        break;
                    case 6:
                        districtInfo = stock.dist06;
                        break;
                    case 7:
                        districtInfo = stock.dist07;
                        break;
                    case 8:
                        districtInfo = stock.dist08;
                        break;
                    case 9:
                        districtInfo = stock.dist09;
                        break;
                    case 10:
                        districtInfo = stock.dist10;
                        break;                    
                }
                result.stockItemUpdates.Add(new StockItemUpdate(warehouseId, itemOrdered.Key, itemOrdered.Value, item.price, districtInfo));
            }
                return Task.FromResult(result);
        }
        
        async Task<float> IWarehouseGrain.Payment(PaymentInfo paymentInformation)
        {
            float total = 0;
            //We need to lookup customer id from last name 
            //TODO: This await can be pushed to the line before the history record addition
            await this.GrainFactory.GetGrain<IWarehouseGrain>(paymentInformation.customerWarehouseId).CustomerPayment(paymentInformation);
            //Update warehouse payment
            state.warehouseRecord.ytd += paymentInformation.paymentAmount;
            total += state.warehouseRecord.ytd;
            //Update district payment
            state.districtRecords[paymentInformation.districtId].ytd += paymentInformation.paymentAmount;
            total += state.districtRecords[paymentInformation.districtId].ytd;
            state.historyRecords.Add(new History(paymentInformation.customerId, paymentInformation.customerDistrictId, paymentInformation.customerWarehouseId, paymentInformation.districtId, 0, paymentInformation.paymentAmount, String.Format("{0,10}     {0,10}", state.warehouseRecord.name, state.districtRecords[paymentInformation.districtId].name)));
            return total;
        }

        async Task<uint> IWarehouseGrain.CustomerPayment(PaymentInfo paymentInformation)
        {
            if (!String.IsNullOrEmpty(paymentInformation.customerLastName))
            {
                paymentInformation.customerId = await this.GrainFactory.GetGrain<IWarehouseGrain>(paymentInformation.customerWarehouseId).FindCustomerId(paymentInformation.customerDistrictId, paymentInformation.customerLastName);
            }
            var customer = state.customerRecords[new Tuple<UInt32, UInt32>(paymentInformation.districtId, paymentInformation.customerId)];
            customer.balance -= paymentInformation.paymentAmount;
            customer.ytdPayment += paymentInformation.paymentAmount;
            customer.paymentCount++;
            if (customer.credit.Substring(0, 2).ToUpper().Equals("BC"))
            {
                //Append new credit line
                String data = "" + paymentInformation.customerId + paymentInformation.customerDistrictId + paymentInformation.customerWarehouseId + paymentInformation.districtId + paymentInformation.warehouseId + paymentInformation.paymentAmount + customer.data;
                customer.data = data;
            }
            return paymentInformation.customerId;
        }

        Task<uint> IWarehouseGrain.FindCustomerId(uint districtId, string customerLastName)
        {
            //Not strictly the same as the original since it requires the customer names to be sorted but will do for now
            var customersWithSameLastName = state.customerNameRecords[new Tuple<UInt32, String>(districtId, customerLastName)];
            return Task.FromResult(customersWithSameLastName[customersWithSameLastName.Count / 2].Item2);
        }
        */
    }
}
