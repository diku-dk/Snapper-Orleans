using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Utilities;

namespace Concurrency.Implementation
{
    public class ScheduleInfo
    {
        public Dictionary<int, ScheduleNode> nodes;
        public ScheduleNode tail; //Points to the last node in the doubly-linked list
        public Dictionary<int, NonDeterministicBatchSchedule> nonDetBatchScheduleMap;
        public Dictionary<int, int> nonDetTxnToScheduleMap;
        
        public ScheduleInfo()
        {
            nodes = new Dictionary<int, ScheduleNode>();
            ScheduleNode node = new ScheduleNode(-1, true);
            nonDetBatchScheduleMap = new Dictionary<int, NonDeterministicBatchSchedule>();
            nonDetTxnToScheduleMap = new Dictionary<int, int>();
            node.promise.SetResult(true);
            tail = node;
            nodes.Add(-1, node);
        }

        public ScheduleNode InsertNonDetTransaction(int tid)
        {
            if(nonDetTxnToScheduleMap.ContainsKey(tid))
            {
                return nodes[nonDetTxnToScheduleMap[tid]].prev;
            }
            
            if (tail.isDet == true)
            {
                ScheduleNode node = new ScheduleNode(tid, false);
                NonDeterministicBatchSchedule schedule = new NonDeterministicBatchSchedule(tid);                
                nonDetBatchScheduleMap.Add(tid, schedule);                
                nodes.Add(tid, node);                
                tail.next = node;
                node.prev = tail;
                tail = node;                
            }            
            nonDetBatchScheduleMap[tail.id].AddTransaction(tid);
            nonDetTxnToScheduleMap.Add(tid, tail.id);
            return tail.prev;
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
                    Debug.Assert(prevNode.id == -1);
                    //Remove nodes connected with -1
                    ScheduleNode next = nodes[-1].next;
                    while(next != null)
                    {
                        Debug.Assert(next.id < schedule.batchID);
                        nodes.Remove(next.id);
                        next = next.next;
                    }
                    //Connect node -1 with the inserted node
                    nodes[-1].next = node;
                    node.prev = nodes[-1];
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

        public HashSet<int> getBeforeSet(int tid)
        {
            var result = new HashSet<int>();
            var node = nodes[nonDetTxnToScheduleMap[tid]].prev;
            while(node.id != -1)
            {                
                if (node.isDet)
                    result.Add(node.id);

                node = node.prev;
            }
            return result;
        }

        public HashSet<int> getAfterSet(int tid)
        {
            var result = new HashSet<int>();
            var node = nodes[nonDetTxnToScheduleMap[tid]].next;
            bool foundAll = false;
            while(node != null)
            {
                if(node.isDet)
                {
                    result.Add(node.id);
                }
                if (node == tail)
                {
                    foundAll = true;
                }
                node = node.next;                
            }

            if(!foundAll)
            {
                foreach(var key in nodes.Keys) {
                    if(key > tid && nodes[key].isDet)
                    {
                        result.Add(key);
                    }
                }
            }
            return result;
        }

        public void completeDeterministicBatch(int id)
        {
            nodes[id].promise.SetResult(true);            
        }

        public void removePreviousNodes(int scheduleId)
        {
            var node = nodes[scheduleId];
            var endOfChainToBeRemoved = node.prev;            

            while(endOfChainToBeRemoved.id != -1)
            {
                nodes.Remove(endOfChainToBeRemoved.id);
                if(!endOfChainToBeRemoved.isDet)
                {
                    nonDetBatchScheduleMap.Remove(endOfChainToBeRemoved.id);                    
                }
                endOfChainToBeRemoved = endOfChainToBeRemoved.prev;
            }
            node.prev = endOfChainToBeRemoved;
            endOfChainToBeRemoved.next = node;
        }

        /*
        //IMPORTANT: Remove the node ahead of me since the next node still depends on me
        private void removePreviousScheduleNode(int scheduleId)
        {
            if(!nodes.ContainsKey(scheduleId))
            {
                throw new Exception($"Schedule does not exist {scheduleId}");
            }
            var node = nodes[scheduleId];
            var nodeToBeRemoved = node.prev;
            if(nodeToBeRemoved.id == -1)
            {
                return;
            } else
            {
                nodeToBeRemoved.prev.next = node;
                node.prev = nodeToBeRemoved.prev;
                nodes.Remove(nodeToBeRemoved.id);
            }            
        }
        */


        public void completeTransaction(int tid)
        {
            var scheduleId = nonDetTxnToScheduleMap[tid];
            nonDetTxnToScheduleMap.Remove(tid);
            var schedule = nonDetBatchScheduleMap[scheduleId];
            if(schedule.RemoveTransaction(tid))
            {
                //Schedule node is completed
                nodes[scheduleId].promise.SetResult(true);
                //Only deterministic batches trigger garbage collection
                //removePreviousScheduleNode(scheduleId);
                //nonDetBatchScheduleMap.Remove(scheduleId);                
            }
        }
    }

    public class ScheduleNode
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
