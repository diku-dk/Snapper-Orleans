using System;
using Utilities;
using TPCC.Interfaces;
using Persist.Interfaces;
using System.Threading.Tasks;
using Concurrency.Implementation;
using System.Collections.Generic;

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

        public async Task<FunctionResult> Init(FunctionInput fin)
        {
            _ = await state.Read(fin.context);
            return new FunctionResult();
        }

        public async Task<FunctionResult> AddNewOrder(FunctionInput fin)
        {
            var context = fin.context;
            var ret = new FunctionResult();
            try
            {
                if (fin.inputObject == null) throw new Exception("Exception: input data is null. ");
                var input = (OrderInfo)fin.inputObject;
                var O_ID = input.order.O_ID;
                var myState = await state.ReadWrite(context);
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
                ret.setException();
            }
            return ret;
        }
    }
}