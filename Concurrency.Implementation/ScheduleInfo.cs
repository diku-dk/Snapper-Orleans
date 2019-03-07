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
        private ScheduleNode tail; //Points to the last node in the doubly-linked list
        private Dictionary<int, NonDeterministicBatchSchedule> nonDetBatchScheduleMap;
        
        public ScheduleInfo()
        {
            nodes = new Dictionary<int, ScheduleNode>();
            ScheduleNode node = new ScheduleNode(-1, true);
            tail = node;
            nodes.Add(-1, node);
        }

        public ScheduleNode inserNonDetTransaction(int tid)
        {
            if (tail.isDet == true)
            {
                ScheduleNode node = new ScheduleNode(tid, false);
                NonDeterministicBatchSchedule schedule = new NonDeterministicBatchSchedule(tid);
                tail.next = node;
                node.prev = tail;
                tail = node;
                nodes.Add(tid, node);
            }
            else
            {
                int id = tail.id;
                this.nonDetBatchScheduleMap[id].AddTransaction(tid);
            }
            return tail;
        }

        public void insertDetBatch(DeterministicBatchSchedule schedule)
        {
            ScheduleNode node;
            if (!nodes.ContainsKey(schedule.batchID))
            {
                node = new ScheduleNode(schedule.batchID, true);
                nodes.Add(schedule.batchID, node);
                
            } else
            {
                node = nodes[schedule.batchID];
            } 
            
            if (nodes.ContainsKey(schedule.lastBatchID))
            {
                ScheduleNode prevNode = nodes[schedule.lastBatchID];
                if(prevNode.next == null)
                {
                    prevNode.next = node;
                    node.prev = prevNode;
                }
                else
                {
                    ScheduleNode prevNext = prevNode.next;
                    prevNext.next = node;
                    node.prev = prevNext;
                }
            }
            else
            {
                ScheduleNode prevNode = new ScheduleNode(schedule.lastBatchID, true);
                nodes.Add(schedule.lastBatchID, prevNode);
                prevNode.next = node;
                node.prev = prevNode;
            }
            if (node.id > tail.id)
                tail = node;
            
        }

        public ScheduleNode find(int id)
        {
            return nodes[id];
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
