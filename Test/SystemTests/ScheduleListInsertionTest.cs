using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Concurrency.Implementation;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Utilities;

namespace Test.SystemTests
{
    [TestClass]
    public class ScheduleListInsertionTest
    {

        [TestMethod]
        public void TestOutOfOrderBatchArrival()
        {
            int N = 10000;
            int[] batches = new int[N];
            int[] order = new int[N];

            //Initializer Batch IDs
            for (int i = 0; i < N; i++)
            {
                batches[i] = i;
                order[i] = i;
            }

            //Shuffle arriving order
            System.Random rnd = new System.Random();
            //order = order.OrderBy(r => rnd.Next()).ToArray();

            Dictionary<int, DeterministicBatchSchedule> schedules = new Dictionary<int, DeterministicBatchSchedule>();
            for (int i = 0; i < batches.Length; i++)
            {
                DeterministicBatchSchedule schedule;
                if (i == 0)
                    schedule = new DeterministicBatchSchedule(batches[i], -1);
                else
                    schedule = new DeterministicBatchSchedule(batches[i], batches[i - 1]);
                schedules.Add(i, schedule);
            }
            ScheduleInfo si = new ScheduleInfo();

            for (int i = 0; i < order.Length; i++)
            {
                si.insertDetBatch(schedules[order[i]]);
            }

            int[] output = new int[N];
            for (int i = -1; i < N - 1; i++)
            {
                output[i + 1] = si.nodes[i].next.id;
            }
            //Console.WriteLine(output.ToString());
            Assert.AreEqual(output, batches);
        }
    }
}
