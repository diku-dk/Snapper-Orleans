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

        public Task<TransactionResult> StartTransaction(string startFunction, FunctionInput inputs)
        {
            AllTxnTypes fnType;
            if (!Enum.TryParse(startFunction.Trim(), out fnType)) throw new FormatException($"Unknown function {startFunction}");
            switch (fnType)
            {
                case AllTxnTypes.Init:
                    return Init(inputs);
                case AllTxnTypes.AddNewOrder:
                    return AddNewOrder(inputs);
                default:
                    throw new Exception($"Unknown function {fnType}");
            }
        }

        // input: Tuple<int, int, int>    W_ID, D_ID, OrderGrain index within the district
        // output: null
        private async Task<TransactionResult> Init(FunctionInput fin)
        {
            var res = new TransactionResult();
            try
            {
                var input = (Tuple<int, int, int>)fin.inputObject;    // W_ID, D_ID, OrderGrain index within the district
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

        private async Task<TransactionResult> AddNewOrder(FunctionInput fin)
        {
            var res = new TransactionResult();
            try
            {
                if (fin.inputObject == null) throw new Exception("Exception: input data is null. ");
                var input = (OrderInfo)fin.inputObject;
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