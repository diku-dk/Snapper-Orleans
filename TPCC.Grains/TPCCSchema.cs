using System;
using System.Collections.Generic;
using System.Text;

namespace TPCC.Grains
{
    [Serializable]
    public class Warehouse
    {
        public UInt32 wareHouseId;
        public float ytd;
        public float tax;
        public String name;
        public String street;
        public String city;
        public String state;
        public String zip;

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
        public UInt32 nextOrderId;
        public String name;
        public String street;
        public String city;
        public String state;
        public String zip;

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
        public String credit;
        public String lastName;
        public String middleName;
        public String firstName;
        public float creditLimit;
        public float balance;
        public float ytdPayment;
        public UInt32 paymentCount;
        public UInt32 deliveryCount;
        public String street1;
        public String street2;
        public String city;
        public String state;
        public String zip;
        public String phone;
        public TimeSpan since;
        public String data;
        
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
        public UInt32 customerId;
        public UInt32 carrierId;
        public UInt16 orderLineCount;
        public bool allLocal;
        public UInt32 entryData;

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
        public UInt32 itemId;
        public UInt32 deliveryDate;
        public float amount;
        public UInt32 supplierWarehouseId;
        public UInt16 quantity;
        public String distInfo;

        public OrderLine(uint itemId, UInt32 deliveryDate, float amount, uint supplierWarehouseId, ushort quantity, string distInfo)
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
        public UInt32 customerId;
        public UInt32 customerDistrictId;
        public UInt32 customerWareHouseId;
        public UInt32 districtId;
        public UInt32 date;
        public float amount;
        public String data;

        public History(uint customerId, uint customerDistrictId, uint customerWareHouseId, uint districtId, UInt32 date, float amount, string data)
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
        public String name;
        public float price;
        public String data;
        public UInt32 imId;

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
        public UInt16 quantity;
        public float ytd;
        public UInt32 orderCount;
        public UInt32 remoteCount;
        public String data;
        public String dist01;
        public String dist02;
        public String dist03;
        public String dist04;
        public String dist05;
        public String dist06;
        public String dist07;
        public String dist08;
        public String dist09;
        public String dist10;

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
        public UInt32 districtId;
        public UInt32 OrderId;

        public NewOrder(uint districtId, uint orderId)
        {
            this.districtId = districtId;
            OrderId = orderId;
        }
    }


    [Serializable]
    public class WarehouseData
    {
        public Warehouse warehouseRecord;
        public Dictionary<UInt32, District> districtRecords;
        public Dictionary<Tuple<UInt32, UInt32>, Customer> customerRecords;
        public Dictionary<Tuple<UInt32, UInt32>, Order> orderRecords;
        public List<NewOrder> newOrders;
        public Dictionary<Tuple<UInt32, UInt32, UInt16>, OrderLine> orderLineRecords;
        public List<History> historyRecords;
        public Dictionary<Tuple<UInt32, String>, List<Tuple<String, UInt32>>> customerNameRecords;
        public Dictionary<UInt32, Item> itemRecords;
        public Dictionary<UInt32, Stock> stockRecords;
    }
}
