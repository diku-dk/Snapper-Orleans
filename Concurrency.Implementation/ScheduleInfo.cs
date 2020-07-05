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
            node.executionPromise.SetResult(true);
            tail = node;
            nodes.Add(-1, node);
        }

        public ScheduleNode InsertNonDetTransaction(int tid)
        {
            if(nonDetTxnToScheduleMap.ContainsKey(tid)) return nodes[nonDetTxnToScheduleMap[tid]].prev;
            
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
            else
            {
                //Join the non-det tail, replace the promise
                if(tail.executionPromise.Task.IsCompleted)
                {
                    tail.executionPromise = new TaskCompletionSource<bool>();
                }
            }
            nonDetBatchScheduleMap[tail.id].AddTransaction(tid);
            nonDetTxnToScheduleMap.Add(tid, tail.id);
            return tail.prev;
        }

        // Yijian's version
        public void insertDetBatch(DeterministicBatchSchedule schedule)
        {
            ScheduleNode node;
            if (!nodes.ContainsKey(schedule.batchID))
            {
                node = new ScheduleNode(schedule.batchID, true);
                nodes.Add(schedule.batchID, node);
            } 
            else node = nodes[schedule.batchID];
            
            if (nodes.ContainsKey(schedule.lastBatchID))
            {
                var prevNode = nodes[schedule.lastBatchID];
                if (prevNode.next == null)
                {
                    prevNode.next = node;
                    node.prev = prevNode;
                }
                else
                {
                    Debug.Assert(prevNode.next.isDet == false && prevNode.next.next == null);
                    prevNode.next.next = node;
                    node.prev = prevNode.next;
                }
            }
            else
            {
                // last node is already deleted because it's committed
                if (schedule.highestCommittedBatchId >= schedule.lastBatchID)
                {
                    var prevNode = nodes[-1];
                    if (prevNode.next == null)
                    {
                        prevNode.next = node;
                        node.prev = prevNode;
                    }
                    else
                    {
                        Debug.Assert(prevNode.next.isDet == false && prevNode.next.next == null);
                        prevNode.next.next = node;
                        node.prev = prevNode.next;
                    }
                }
                else   // last node hasn't arrived yet
                {
                    var prevNode = new ScheduleNode(schedule.lastBatchID, true);
                    nodes.Add(schedule.lastBatchID, prevNode);
                    prevNode.next = node;
                    node.prev = prevNode;
                    // at this time, prevNode.prev == null (Yijian)
                }
            }

            if (node.id > tail.id) tail = node;
        }

        public TaskCompletionSource<Boolean> getDependingPromise(int id)
        {
            Debug.Assert(nodes[id].prev != null);
            return nodes[id].prev.executionPromise;
        }

        public HashSet<int> getBeforeSet(int tid, out int maxBeforeBid)
        {
            var result = new HashSet<int>();
            var node = nodes[nonDetTxnToScheduleMap[tid]].prev;
            maxBeforeBid = node.id == -1 ? int.MinValue : node.id;
            while(node.id != -1 && !node.commitmentPromise.Task.IsCompleted)
            {   
                if (node.isDet) result.Add(node.id);
                node = node.prev;
            }
            return result;
        }

        // TODO: changed by Yijian
        public HashSet<int> getAfterSet(int tid, int maxBeforeBid, out int minAfterBid)
        {
            minAfterBid = int.MaxValue;
            var result = new HashSet<int>();
            foreach (var key in nodes.Keys)
            {
                if (key > maxBeforeBid && nodes[key].isDet && key > -1)
                {
                    result.Add(key);
                    minAfterBid = minAfterBid > key ? key : minAfterBid;
                }
            }
            return result;
        }

        public void completeDeterministicBatch(int id)
        {
            nodes[id].executionPromise.SetResult(true);            
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

        public void completeTransaction(int tid)
        {
            var scheduleId = nonDetTxnToScheduleMap[tid];
            nonDetTxnToScheduleMap.Remove(tid);
            var schedule = nonDetBatchScheduleMap[scheduleId];
            if(schedule.RemoveTransaction(tid))
            {
                //Schedule node is completed
                nodes[scheduleId].executionPromise.SetResult(true);
                //Only deterministic batches trigger garbage collection               
            }
        }
    }

    public class ScheduleNode
    {
        public int id;
        public bool isDet = false;
        public TaskCompletionSource<Boolean> commitmentPromise = new TaskCompletionSource<bool>(false);
        public TaskCompletionSource<Boolean> executionPromise = new TaskCompletionSource<bool>(false);
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
