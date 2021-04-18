using System;
using Utilities;
using TPCC.Interfaces;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace TPCC.Grains
{
    public class EventualOrderGrain : Orleans.Grain, IEventualOrderGrain
    {
        OrderData state = new OrderData();

        public Task<TransactionResult> StartTransaction(string startFunc, object funcInput)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunc.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunc}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(funcInput);
                case AllTxnTypes.AddNewOrder:
                    return AddNewOrder(funcInput);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input: Tuple<int, int, int>    W_ID, D_ID, OrderGrain index within the district
        // output: null
        private async Task<TransactionResult> Init(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int, int>)funcInput;    // W_ID, D_ID, OrderGrain index within the district
                var myState = state;
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

        private async Task<TransactionResult> AddNewOrder(object funcInput)
        {
            var res = new TransactionResult();
            try
            {
                if (funcInput == null) throw new Exception("Exception: input data is null. ");
                var input = (OrderInfo)funcInput;
                var O_ID = input.order.O_ID;
                var myState = state;
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