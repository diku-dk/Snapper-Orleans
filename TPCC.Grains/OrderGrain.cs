using System;
using Utilities;
using TPCC.Interfaces;
using Persist.Interfaces;
using System.Threading.Tasks;
using Concurrency.Implementation;
using System.Collections.Generic;
using System.Diagnostics;

namespace TPCC.Grains
{
    [Serializable]
    public class OrderInfo
    {
        public Order order;
        public List<OrderLine> orderlines;

        public OrderInfo(Order order, List<OrderLine> orderlines)
        {
            this.order = order;
            this.orderlines = orderlines;
        }
    }

    [Serializable]
    public class OrderData : ICloneable
    {
        public int W_ID;
        public int D_ID;
        public int OrderGrainID;
        public List<long> neworder;
        public Dictionary<long, Order> order_table;                      // key: O_ID
        public Dictionary<Tuple<long, int>, OrderLine> orderline_table;  // key: <O_ID, number of items in the order>

        public OrderData()
        {
            neworder = new List<long>();
            order_table = new Dictionary<long, Order>();
            orderline_table = new Dictionary<Tuple<long, int>, OrderLine>();
        }

        public OrderData(OrderData orderdata)
        {
            W_ID = orderdata.W_ID;
            D_ID = orderdata.D_ID;
            OrderGrainID = orderdata.OrderGrainID;
            neworder = new List<long>(orderdata.neworder);
            order_table = new Dictionary<long, Order>(orderdata.order_table);
            orderline_table = new Dictionary<Tuple<long, int>, OrderLine>(orderdata.orderline_table);
        }

        object ICloneable.Clone()
        {
            return new OrderData(this);
        }
    }

    public class OrderGrain : TransactionExecutionGrain<OrderData>, IOrderGrain
    {
        public OrderGrain(IPersistSingletonGroup persistSingletonGroup) : base(persistSingletonGroup, "TPCC.Grains.OrderGrain")
        {
        }

        // input: Tuple<int, int, int>    W_ID, D_ID, OrderGrain index within the district
        // output: null
        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            var context = fin.context;
            var res = new FunctionResult();
            try
            {
                var input = (Tuple<int, int, int>)fin.inputObject;    // W_ID, D_ID, OrderGrain index within the district
                var myState = await state.ReadWrite(context);
                myState.W_ID = input.Item1;
                myState.D_ID = input.Item2;
                myState.OrderGrainID = input.Item3;
                myState.neworder = new List<long>();
                myState.order_table = new Dictionary<long, Order>();
                myState.orderline_table = new Dictionary<Tuple<long, int>, OrderLine>();
            }
            catch (Exception e)
            {
                res.setException();
            }
            return res;
        }

        public async Task<FunctionResult> AddNewOrder(FunctionInput fin)
        {
            var context = fin.context;
            var res = new FunctionResult();
            try
            {
                if (fin.inputObject == null) throw new Exception("Exception: input data is null. ");
                var input = (OrderInfo)fin.inputObject;
                var O_ID = input.order.O_ID;
                var myState = await state.ReadWrite(context);
                //if (myState.neworder.Contains(O_ID)) Console.WriteLine($"OrderGrain: W_ID = {myState.W_ID}, D_ID = {myState.D_ID}, index = {myState.OrderGrainID}, for C_ID = {input.order.O_C_ID}, the O_ID = {O_ID} already exists. ");
                Debug.Assert(myState.neworder.Contains(O_ID) == false);
                myState.neworder.Add(O_ID);
                myState.order_table.Add(O_ID, input.order);
                foreach (var orderline in input.orderlines)
                {
                    var num = orderline.OL_NUMBER;
                    myState.orderline_table.Add(new Tuple<long, int>(O_ID, num), orderline);
                }
            }
            catch (Exception e)
            {
                //Console.WriteLine($"Exception: {e.Message}, {e.StackTrace}");
                res.setException();
            }
            return res;
        }
    }
}