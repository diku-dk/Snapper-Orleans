using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation
{
    class ScheduleInfo
    {
        private Dictionary<int, ScheduleNode> nodes;
        private Dictionary<int, ScheduleNode> heads;
        private ScheduleNode tail; //Points to the last node in the doubly-linked list

        public void insert(ScheduleNode node)
        {

        }

        public ScheduleNode find(int id)
        {
            return null;
        }

        public void completeDeterministicBatch(int id)
        {
            nodes[id].promise.SetResult(true);
        }
    }

    class ScheduleNode
    {
        public int id;
        public bool isDet = false;
        public TaskCompletionSource<Boolean> promise = new TaskCompletionSource<bool>(false);
        //links
        public ScheduleNode prev;
        public ScheduleNode next;

        public ScheduleNode(int id, bool isDet)
        {
            this.id = id;
            this.isDet = isDet;
        }
    }
}
