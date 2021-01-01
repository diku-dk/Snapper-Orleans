using System;
using System.Collections.Generic;

namespace TPCC.Grains
{
    [Serializable]
    public class Warehouse
    {
        public int W_ID;        // primary key
        public string W_NAME;
        public string W_STREET_1;
        public string W_STREET_2;
        public string W_CITY;
        public string W_STATE;
        public string W_ZIP;
        public float W_TAX;
        public float W_YTD;

        public Warehouse(int W_ID, string W_NAME, string W_STREET_1, string W_STREET_2, string W_CITY, string W_STATE, string W_ZIP, float W_TAX, float W_YTD)
        {
            this.W_ID = W_ID;
            this.W_NAME = W_NAME;
            this.W_STREET_1 = W_STREET_1;
            this.W_STREET_2 = W_STREET_2;
            this.W_CITY = W_CITY;
            this.W_STATE = W_STATE;
            this.W_ZIP = W_ZIP;
            this.W_TAX = W_TAX;
            this.W_YTD = W_YTD;
        }
    }

    [Serializable]
    public class District
    {
        public int D_ID;  // primary key

        public string D_NAME;
        public string D_STREET_1;
        public string D_STREET_2;
        public string D_CITY;
        public string D_STATE;
        public string D_ZIP;
        public float D_TAX;
        public float D_YTD;
        public int D_NEXT_O_ID;

        public District(int D_ID, string D_NAME, string D_STREET_1, string D_STREET_2, string D_CITY, string D_STATE, string D_ZIP, float D_TAX, float D_YTD, int D_NEXT_O_ID)
        {
            this.D_ID = D_ID;
            this.D_NAME = D_NAME;
            this.D_STREET_1 = D_STREET_1;
            this.D_STREET_2 = D_STREET_2;
            this.D_CITY = D_CITY;
            this.D_STATE = D_STATE;
            this.D_ZIP = D_ZIP;
            this.D_TAX = D_TAX;
            this.D_YTD = D_YTD;
            this.D_NEXT_O_ID = D_NEXT_O_ID;
        }
    }

    [Serializable]
    public class Customer
    {
        public int C_ID;  // primary key

        public string C_FIRST;
        public string C_MIDDLE;
        public string C_LAST;
        public string C_STREET_1;
        public string C_STREET_2;
        public string C_CITY;
        public string C_STATE;
        public string C_ZIP;
        public string C_PHONE;
        public DateTime C_SINCE;
        public string C_CREDIT;
        public float C_CREDIT_LIM;
        public float C_DISCOUNT;
        public float C_BALANCE;
        public float C_YTD_PAYMENT;
        public int C_PAYMENT_CNT;
        public int C_DELIVERY_CNT;
        public string C_DATA;
        
        public Customer(int C_ID, string C_FIRST, string C_MIDDLE, string C_LAST, string C_STREET_1, string C_STREET_2, string C_CITY, string C_STATE, string C_ZIP, string C_PHONE, DateTime C_SINCE, string C_CREDIT, float C_CREDIT_LIM, float C_DISCOUNT, float C_BALANCE, float C_YTD_PAYMENT, int C_PAYMENT_CNT, int C_DELIVERY_CNT, string C_DATA)
        {
            this.C_ID = C_ID;
            this.C_FIRST = C_FIRST;
            this.C_MIDDLE = C_MIDDLE;
            this.C_LAST = C_LAST;
            this.C_STREET_1 = C_STREET_1;
            this.C_STREET_2 = C_STREET_2;
            this.C_CITY = C_CITY;
            this.C_STATE = C_STATE;
            this.C_ZIP = C_ZIP;
            this.C_PHONE = C_PHONE;
            this.C_SINCE = C_SINCE;
            this.C_CREDIT = C_CREDIT;
            this.C_CREDIT_LIM = C_CREDIT_LIM;
            this.C_DISCOUNT = C_DISCOUNT;
            this.C_BALANCE = C_BALANCE;
            this.C_YTD_PAYMENT = C_YTD_PAYMENT;
            this.C_PAYMENT_CNT = C_PAYMENT_CNT;
            this.C_DELIVERY_CNT = C_DELIVERY_CNT;
            this.C_DATA = C_DATA;
        }
    }

    [Serializable]
    public class History
    {
        // no primary key
        public int H_C_ID;
        public int H_C_D_ID;
        public int H_C_W_ID;
        public int H_D_ID;
        public int H_W_ID;
        public DateTime H_DATE;
        public float H_AMOUNT;
        public string H_DATA;

        public History(int H_C_ID, int H_C_D_ID, int H_C_W_ID, int H_D_ID, int H_W_ID, DateTime H_DATE, float H_AMOUNT, string H_DATA)
        {
            this.H_C_ID = H_C_ID;
            this.H_C_D_ID = H_C_D_ID;
            this.H_C_W_ID = H_C_W_ID;
            this.H_D_ID = H_D_ID;
            this.H_W_ID = H_W_ID;
            this.H_DATE = H_DATE;
            this.H_AMOUNT = H_AMOUNT;
            this.H_DATA = H_DATA;
        }
    }

    [Serializable]
    public class NewOrder
    {
        public int NO_O_ID;  // primary key

        public NewOrder(int NO_O_ID)
        {
            this.NO_O_ID = NO_O_ID;
        }
    }

    [Serializable]
    public class Order
    {
        public int O_ID;  // primary key

        public int O_C_ID;
        public DateTime O_ENTRY_D;
        public int O_CARRIER_ID;
        public int O_OL_CNT;
        public bool O_ALL_LOCAL;

        public Order(int O_ID, int O_C_ID, DateTime O_ENTRY_D, object O_CARRIER_ID, int O_OL_CNT, bool O_ALL_LOCAL)
        {
            this.O_ID = O_ID;
            this.O_C_ID = O_C_ID;
            this.O_ENTRY_D = O_ENTRY_D;
            if (O_CARRIER_ID != null) this.O_CARRIER_ID = (int)O_CARRIER_ID;
            this.O_OL_CNT = O_OL_CNT;
            this.O_ALL_LOCAL = O_ALL_LOCAL;
        }
    }

    [Serializable]
    public class OrderLine
    {
        // primary key
        public int OL_O_ID;
        public int OL_NUMBER;  // ???????

        public int OL_I_ID;
        public int OL_SUPPLY_W_ID;
        public DateTime OL_DELIVERY_D;
        public int OL_QUANTITY;
        public float OL_AMOUNT;
        public string OL_DIST_INFO;

        public OrderLine(int OL_O_ID, int OL_NUMBER, int OL_I_ID, int OL_SUPPLY_W_ID, object OL_DELIVERY_D, int OL_QUANTITY, float OL_AMOUNT, string OL_DIST_INFO)
        {
            this.OL_O_ID = OL_O_ID;
            this.OL_NUMBER = OL_NUMBER;
            this.OL_I_ID = OL_I_ID;
            this.OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
            if (OL_DELIVERY_D != null) this.OL_DELIVERY_D = (DateTime)OL_DELIVERY_D;
            this.OL_QUANTITY = OL_QUANTITY;
            this.OL_AMOUNT = OL_AMOUNT;
            this.OL_DIST_INFO = OL_DIST_INFO;
        }
    }

    [Serializable]
    public class Item
    {
        public int I_ID;   // primary key
        public int I_IM_ID;
        public string I_NAME;
        public float I_PRICE;
        public string I_DATA;

        public Item(int I_ID, int I_IM_ID, string I_NAME, float I_PRICE, string I_DATA)
        {
            this.I_ID = I_ID;
            this.I_IM_ID = I_IM_ID;
            this.I_NAME = I_NAME;
            this.I_PRICE = I_PRICE;
            this.I_DATA = I_DATA;
        }
    }

    [Serializable]
    public class Stock
    {
        public int S_I_ID;  // primary key

        public int S_QUANTITY;
        public Dictionary<int, string> S_DIST;
        public int S_YTD;
        public int S_ORDER_CNT;
        public int S_REMOTE_CNT;
        public string S_DATA;

        public Stock(int S_I_ID, int S_QUANTITY, Dictionary<int, string> S_DIST, int S_YTD, int S_ORDER_CNT, int S_REMOTE_CNT, string S_DATA)
        {
            this.S_I_ID = S_I_ID;
            this.S_QUANTITY = S_QUANTITY;
            this.S_DIST = S_DIST;
            this.S_YTD = S_YTD;
            this.S_ORDER_CNT = S_ORDER_CNT;
            this.S_REMOTE_CNT = S_REMOTE_CNT;
            this.S_DATA = S_DATA;
        }
    }

    [Serializable]
    public class WarehouseData : ICloneable
    {
        public Warehouse warehouse_info;
        public District district_info;
        public Dictionary<int, Customer> customer_table;                // key: C_ID
        public List<History> history;
        public List<NewOrder> neworder;
        public Dictionary<int, Order> order_table;                      // key: O_ID
        public Dictionary<Tuple<int, int>, OrderLine> orderline_table;  // key: <O_ID, NUMBER>
        public Dictionary<int, Item> item_table;                        // key: I_ID
        public Dictionary<int, Stock> stock_table;                      // key: I_ID

        public WarehouseData()
        {
            customer_table = new Dictionary<int, Customer>();
            history = new List<History>();
            neworder = new List<NewOrder>();
            order_table = new Dictionary<int, Order>();
            orderline_table = new Dictionary<Tuple<int, int>, OrderLine>();
            item_table = new Dictionary<int, Item>();
            stock_table = new Dictionary<int, Stock>();
        }

        public WarehouseData(WarehouseData warehouse)
        {
            warehouse_info = warehouse.warehouse_info;
            district_info = warehouse.district_info;
            customer_table = warehouse.customer_table;
            history = warehouse.history;
            neworder = warehouse.neworder;
            order_table = warehouse.order_table;
            orderline_table = warehouse.orderline_table;
            item_table = warehouse.item_table;
            stock_table = warehouse.stock_table;
        }

        object ICloneable.Clone()
        {
            return new WarehouseData(this);
        }
    }
}
