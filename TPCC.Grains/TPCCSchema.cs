using System;
using System.Collections.Generic;

namespace TPCC.Grains
{
    [Serializable]
    public class Warehouse
    {
        public uint wareHouseId;
        public float ytd;
        public float tax;
        public string name;
        public string street;
        public string city;
        public string state;
        public string zip;

        public Warehouse(uint wareHouseId, float ytd, float tax, string name, string street, string city, string state, string zip)
        {
            this.wareHouseId = wareHouseId;
            this.ytd = ytd;
            this.tax = tax;
            this.name = name;
            this.street = street;
            this.city = city;
            this.state = state;
            this.zip = zip;
        }
    }

    [Serializable]
    public class District
    {
        public float ytd;
        public float tax;
        public uint nextOrderId;
        public string name;
        public string street;
        public string city;
        public string state;
        public string zip;

        public District(float ytd, float tax, uint nextOrderId, string name, string street, string city, string state, string zip)
        {
            this.ytd = ytd;
            this.tax = tax;
            this.nextOrderId = nextOrderId;
            this.name = name;
            this.street = street;
            this.city = city;
            this.state = state;
            this.zip = zip;
        }
    }

    [Serializable]
    public class Customer
    {
        public float discount;
        public string credit;
        public string lastName;
        public string middleName;
        public string firstName;
        public float creditLimit;
        public float balance;
        public float ytdPayment;
        public uint paymentCount;
        public uint deliveryCount;
        public string street1;
        public string street2;
        public string city;
        public string state;
        public string zip;
        public string phone;
        public TimeSpan since;
        public string data;
        
        public Customer(float discount, string credit, string lastName, string middleName, string firstName, float creditLimit, float balance, float ytdPayment, uint paymentCount, uint deliveryCount, string street1, string street2, string city, string state, string zip, string phone, TimeSpan since, string data)
        {
            this.discount = discount;
            this.credit = credit;
            this.lastName = lastName;
            this.middleName = middleName;
            this.firstName = firstName;
            this.creditLimit = creditLimit;
            this.balance = balance;
            this.ytdPayment = ytdPayment;
            this.paymentCount = paymentCount;
            this.deliveryCount = deliveryCount;
            this.street1 = street1;
            this.street2 = street2;
            this.city = city;
            this.state = state;
            this.zip = zip;
            this.phone = phone;
            this.since = since;
            this.data = data;
        }
    }

    [Serializable]
    public class Order
    {
        public uint customerId;
        public uint carrierId;
        public ushort orderLineCount;
        public bool allLocal;
        public uint entryData;

        public Order(uint customerId, uint carrierId, ushort orderLineCount, bool allLocal, uint entryData)
        {
            this.customerId = customerId;
            this.carrierId = carrierId;
            this.orderLineCount = orderLineCount;
            this.allLocal = allLocal;
            this.entryData = entryData;
        }
    }

    [Serializable]
    public class OrderLine
    {
        public uint itemId;
        public uint deliveryDate;
        public float amount;
        public uint supplierWarehouseId;
        public ushort quantity;
        public string distInfo;

        public OrderLine(uint itemId, uint deliveryDate, float amount, uint supplierWarehouseId, ushort quantity, string distInfo)
        {
            this.itemId = itemId;
            this.deliveryDate = deliveryDate;
            this.amount = amount;
            this.supplierWarehouseId = supplierWarehouseId;
            this.quantity = quantity;
            this.distInfo = distInfo;
        }
    }

    [Serializable]
    public class History
    {
        public uint customerId;
        public uint customerDistrictId;
        public uint customerWareHouseId;
        public uint districtId;
        public uint date;
        public float amount;
        public string data;

        public History(uint customerId, uint customerDistrictId, uint customerWareHouseId, uint districtId, uint date, float amount, string data)
        {
            this.customerId = customerId;
            this.customerDistrictId = customerDistrictId;
            this.customerWareHouseId = customerWareHouseId;
            this.districtId = districtId;
            this.date = date;
            this.amount = amount;
            this.data = data;
        }
    }

    [Serializable]
    public class Item
    {
        public string name;
        public float price;
        public string data;
        public uint imId;

        public Item(string name, float price, string data, uint imId)
        {
            this.name = name;
            this.price = price;
            this.data = data;
            this.imId = imId;
        }
    }

    [Serializable]
    public class Stock
    {
        public ushort quantity;
        public float ytd;
        public uint orderCount;
        public uint remoteCount;
        public string data;
        public string dist01;
        public string dist02;
        public string dist03;
        public string dist04;
        public string dist05;
        public string dist06;
        public string dist07;
        public string dist08;
        public string dist09;
        public string dist10;

        public Stock(ushort quantity, float ytd, uint orderCount, uint remoteCount, string data, string dist01, string dist02, string dist03, string dist04, string dist05, string dist06, string dist07, string dist08, string dist09, string dist10)
        {
            this.quantity = quantity;
            this.ytd = ytd;
            this.orderCount = orderCount;
            this.remoteCount = remoteCount;
            this.data = data;
            this.dist01 = dist01;
            this.dist02 = dist02;
            this.dist03 = dist03;
            this.dist04 = dist04;
            this.dist05 = dist05;
            this.dist06 = dist06;
            this.dist07 = dist07;
            this.dist08 = dist08;
            this.dist09 = dist09;
            this.dist10 = dist10;
        }
    }

    [Serializable]
    public class NewOrder
    {
        public uint districtId;
        public uint OrderId;

        public NewOrder(uint districtId, uint orderId)
        {
            this.districtId = districtId;
            OrderId = orderId;
        }
    }


    [Serializable]
    public class WarehouseData : ICloneable
    {
        public Warehouse warehouseRecord;
        public Dictionary<uint, District> districtRecords;
        public Dictionary<Tuple<uint, uint>, Customer> customerRecords;
        public Dictionary<Tuple<uint, uint>, Order> orderRecords;
        public List<NewOrder> newOrders;
        public Dictionary<Tuple<uint, uint, ushort>, OrderLine> orderLineRecords;
        public List<History> historyRecords;
        public Dictionary<Tuple<uint, string>, List<Tuple<string, uint>>> customerNameRecords;
        public Dictionary<uint, Item> itemRecords;
        public Dictionary<uint, Stock> stockRecords;

        public WarehouseData()
        {
        }

        public WarehouseData(WarehouseData warehouse)
        {
            warehouseRecord = warehouse.warehouseRecord;
            districtRecords = warehouse.districtRecords;
            customerRecords = warehouse.customerRecords;
            orderRecords = warehouse.orderRecords;
            newOrders = warehouse.newOrders;
            orderLineRecords = warehouse.orderLineRecords;
            historyRecords = warehouse.historyRecords;
            customerNameRecords = warehouse.customerNameRecords;
            itemRecords = warehouse.itemRecords;
            stockRecords = warehouse.stockRecords;
        }

        object ICloneable.Clone()
        {
            return new WarehouseData(this);
        }
    }
}
