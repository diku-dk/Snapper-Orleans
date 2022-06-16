using System;
using Utilities;
using TPCC.Interfaces;
using System.Diagnostics;
using System.Threading.Tasks;
using Concurrency.Implementation.TransactionExecution;
using System.Collections.Generic;
using Concurrency.Interface.Logging;
using System.Runtime.Serialization;
using Concurrency.Interface.Coordinator;

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
    public class OrderData : ICloneable, ISerializable
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

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("W_ID", W_ID, typeof(int));
            info.AddValue("D_ID", D_ID, typeof(int));
            info.AddValue("OrderGrainID", OrderGrainID, typeof(int));
            info.AddValue("neworder", neworder, typeof(List<long>));
            info.AddValue("order_table", order_table, typeof(Dictionary<long, Order>));
            info.AddValue("orderline_table", orderline_table, typeof(Dictionary<Tuple<long, int>, OrderLine>));
        }

        object ICloneable.Clone()
        {
            return new OrderData(this);
        }
    }

    public class OrderGrain : TransactionExecutionGrain<OrderData>, IOrderGrain
    {
        public OrderGrain(ILoggerGroup loggerGroup, ICoordMap coordMap) : base(loggerGroup, coordMap, "TPCC.Grains.OrderGrain")
        {
        }

        // input: Tuple<int, int, int>    W_ID, D_ID, OrderGrain index within the district
        // output: null
        public async Task<TransactionResult> Init(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int, int>)funcInput;    // W_ID, D_ID, OrderGrain index within the district
                var myState = await GetState(context, AccessMode.ReadWrite);
                myState.W_ID = input.Item1;
                myState.D_ID = input.Item2;
                myState.OrderGrainID = input.Item3;
                myState.neworder = new List<long>();
                myState.order_table = new Dictionary<long, Order>();
                myState.orderline_table = new Dictionary<Tuple<long, int>, OrderLine>();
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }

        public async Task<TransactionResult> AddNewOrder(TransactionContext context, object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                if (funcInput == null) throw new Exception("Exception: input data is null. ");
                var input = (OrderInfo)funcInput;
                var O_ID = input.order.O_ID;
                var myState = await GetState(context, AccessMode.ReadWrite);
                Debug.Assert(myState.neworder.Contains(O_ID) == false);
                myState.neworder.Add(O_ID);
                myState.order_table.Add(O_ID, input.order);
                foreach (var orderline in input.orderlines)
                {
                    var num = orderline.OL_NUMBER;
                    myState.orderline_table.Add(new Tuple<long, int>(O_ID, num), orderline);
                }
            }
            catch (Exception)
            {
                res.exception = true;
            }
            return res;
        }
    }
}