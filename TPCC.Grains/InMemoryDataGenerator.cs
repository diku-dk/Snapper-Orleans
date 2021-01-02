using System;
using System.Collections.Generic;
using System.Linq;
using Utilities;

namespace TPCC.Grains
{
    public class InMemoryDataGenerator
    {
        static Random random = new Random();
        const string numbers = "0123456789";
        const string alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        public static void GenerateSimpleData(int W_ID, int D_ID, WarehouseData data)
        {
            // generate data for Warehouse table
            var W_NAME = "a";
            var W_STREET_1 = "a";
            var W_STREET_2 = "a";
            var W_CITY = "a";
            var W_STATE = "aa";
            var W_ZIP = "aaaaaaaaa";
            var W_TAX = numeric(4, 4, true);
            var W_YTD = numeric(12, 2, true);
            data.warehouse_info = new Warehouse(W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD);

            // generate data for District table
            string D_NAME = "a";
            var D_STREET_1 = "a";
            var D_STREET_2 = "a";
            var D_CITY = "a";
            var D_STATE = "aa";
            var D_ZIP = "aaaaaaaaa";
            var D_TAX = numeric(4, 4, true);
            var D_YTD = numeric(12, 2, true);
            var D_NEXT_O_ID = random.Next(0, 10000000);
            data.district_info = new District(D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID);

            // generate data for Customer table
            for (int i = 0; i < Constants.NUM_C_PER_D; i++)
            {
                var C_ID = i;
                var C_FIRST = "a";
                var C_MIDDLE = "aa";
                var C_LAST = "a";
                var C_STREET_1 = "a";
                var C_STREET_2 = "a";
                var C_CITY = "a";
                var C_STATE = "aa";
                var C_ZIP = "aaaaaaaaa";
                var C_PHONE = "aaaaaaaaaaaaaaaa";
                var C_SINCE = DateTime.Now;
                var C_CREDIT = "aa";
                var C_CREDIT_LIM = numeric(12, 2, true);
                var C_DISCOUNT = numeric(4, 4, true);
                var C_BALANCE = numeric(12, 2, true);
                var C_YTD_PAYMENT = numeric(12, 2, true);
                var C_PAYMENT_CNT = numeric(4, false);
                var C_DELIVERY_CNT = numeric(4, false);
                var C_DATA = "a";
                data.customer_table.Add(C_ID, new Customer(C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA));
            }

            // generate data for Item table
            var NUM_I_PER_D = Constants.NUM_I / Constants.NUM_D_PER_W;
            for (int i = 0; i < NUM_I_PER_D; i++)
            {
                var I_ID = i * Constants.NUM_D_PER_W + D_ID;
                var I_IM_ID = I_ID;
                var I_NAME = "a";
                var I_PRICE = numeric(5, 2, false);
                var I_DATA = "a";
                data.item_table.Add(I_ID, new Item(I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA));
            }

            // generate data for Stock table
            for (int i = 0; i < NUM_I_PER_D; i++)
            {
                var S_I_ID = i * Constants.NUM_D_PER_W + D_ID;
                var S_QUANTITY = numeric(4, true);
                var S_DIST = new Dictionary<int, string>();
                for (int d = 0; d < Constants.NUM_D_PER_W; d++) S_DIST.Add(d, "aaaaaaaaaaaaaaaaaaaaaaaa");
                var S_YTD = numeric(8, false);
                var S_ORDER_CNT = numeric(4, false);
                var S_REMOTE_CNT = numeric(4, false);
                var S_DATA = "a";
                data.stock_table.Add(S_I_ID, new Stock(S_I_ID, S_QUANTITY, S_DIST, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA));
            }
        }

        public static void GenerateData(int W_ID, int D_ID, WarehouseData data)
        {
            // generate data for Warehouse table
            var W_NAME = RandomString(1, alphanumeric);
            var W_STREET_1 = RandomString(1, alphanumeric);
            var W_STREET_2 = RandomString(1, alphanumeric);
            var W_CITY = RandomString(1, alphanumeric);
            var W_STATE = RandomString(2, alphanumeric);
            var W_ZIP = RandomString(9, alphanumeric);
            var W_TAX = numeric(4, 4, true);
            var W_YTD = numeric(12, 2, true);
            data.warehouse_info = new Warehouse(W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD);

            // generate data for District table
            string D_NAME = RandomString(1, alphanumeric);
            var D_STREET_1 = RandomString(1, alphanumeric);
            var D_STREET_2 = RandomString(1, alphanumeric);
            var D_CITY = RandomString(1, alphanumeric);
            var D_STATE = RandomString(2, alphanumeric);
            var D_ZIP = RandomString(9, alphanumeric);
            var D_TAX = numeric(4, 4, true);
            var D_YTD = numeric(12, 2, true);
            var D_NEXT_O_ID = random.Next(0, 10000000);
            data.district_info = new District(D_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID);

            // generate data for Customer table
            for (int i = 0; i < Constants.NUM_C_PER_D; i++)
            {
                var C_ID = i;
                var C_FIRST = RandomString(1, alphanumeric);
                var C_MIDDLE = RandomString(2, alphanumeric);
                var C_LAST = RandomString(1, alphanumeric);
                var C_STREET_1 = RandomString(1, alphanumeric);
                var C_STREET_2 = RandomString(1, alphanumeric);
                var C_CITY = RandomString(1, alphanumeric);
                var C_STATE = RandomString(2, alphanumeric);
                var C_ZIP = RandomString(9, alphanumeric);
                var C_PHONE = RandomString(16, alphanumeric);
                var C_SINCE = DateTime.Now;
                var C_CREDIT = RandomString(2, alphanumeric);
                var C_CREDIT_LIM = numeric(12, 2, true);
                var C_DISCOUNT = numeric(4, 4, true);
                var C_BALANCE = numeric(12, 2, true);
                var C_YTD_PAYMENT = numeric(12, 2, true);
                var C_PAYMENT_CNT = numeric(4, false);
                var C_DELIVERY_CNT = numeric(4, false);
                var C_DATA = RandomString(1, alphanumeric);
                data.customer_table.Add(C_ID, new Customer(C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA));
            }

            // generate data for Item table
            var NUM_I_PER_D = Constants.NUM_I / Constants.NUM_D_PER_W;
            for (int i = 0; i < NUM_I_PER_D; i++)
            {
                var I_ID = i * Constants.NUM_D_PER_W + D_ID;
                var I_IM_ID = I_ID;
                var I_NAME = RandomString(1, alphanumeric);
                var I_PRICE = numeric(5, 2, false);
                var I_DATA = RandomString(1, alphanumeric);
                data.item_table.Add(I_ID, new Item(I_ID, I_IM_ID, I_NAME, I_PRICE, I_DATA));
            }

            // generate data for Stock table
            for (int i = 0; i < NUM_I_PER_D; i++)
            {
                var S_I_ID = i * Constants.NUM_D_PER_W + D_ID;
                var S_QUANTITY = numeric(4, true);
                var S_DIST = new Dictionary<int, string>();
                for (int d = 0; d < Constants.NUM_D_PER_W; d++) S_DIST.Add(d, RandomString(24, alphanumeric));
                var S_YTD = numeric(8, false);
                var S_ORDER_CNT = numeric(4, false);
                var S_REMOTE_CNT = numeric(4, false);
                var S_DATA = RandomString(1, alphanumeric);
                data.stock_table.Add(S_I_ID, new Stock(S_I_ID, S_QUANTITY, S_DIST, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA));
            }
        }

        private static string RandomString(int length, string chars)
        {
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private static int numeric(int m, bool signed)
        {
            var num = random.Next((int)Math.Pow(10, m), (int)Math.Pow(10, m + 1));
            var isPositive = random.Next(0, 2);
            if (signed && isPositive > 1) return -num;
            else return num;
        }

        private static float numeric(int m, int n, bool signed)   // float 6-9位小数，double 15-17位小数
        {
            float the_number;
            var str = RandomString(m, numbers);
            if (m == n) the_number = float.Parse("0." + str);
            else if (m > n)
            {
                var left = str.Substring(0, m - n);
                var right = str.Substring(m - n);
                the_number = float.Parse(left + "." + right);
            }
            else
            {
                var left = "0.";
                for (int i = 0; i < n - m; i++) left += "0";
                the_number = float.Parse(left + str);
            }
            var isPositive = random.Next(0, 2);
            if (signed && isPositive > 0) return -the_number;
            return the_number;
        }
    }
}
