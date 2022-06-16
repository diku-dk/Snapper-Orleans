﻿using Utilities;
using System.Diagnostics;
using System.Threading.Tasks;
using System.Collections.Generic;
using System;

namespace Concurrency.Implementation.TransactionExecution
{
    public class ScheduleInfo
    {
        readonly int myID;

        public Dictionary<long, ScheduleNode> detNodes;     // node ID: batch ID (local bid generated by local coordinators)
        public Dictionary<long, long> localBidToGlobalBid;   // local bid, global bid

        public Dictionary<long, ScheduleNode> nonDetNodes;  // node ID: ACT tid
        // in single-silo deployment, ACT tid is generated by coordinators in the silo
        // in multi-silo deployment, ACT tid is generated by global coordinators, since it is unknown if the ACT is a local or global transaction
        public Dictionary<long, long> nonDetTidToNodeID;
        public Dictionary<long, HashSet<long>> nonDetNodeIDToTxnSet;
        
        public ScheduleInfo(int myID)
        {
            this.myID = myID;
            detNodes = new Dictionary<long, ScheduleNode>();
            localBidToGlobalBid = new Dictionary<long, long>();

            nonDetNodes = new Dictionary<long, ScheduleNode>();
            nonDetNodeIDToTxnSet = new Dictionary<long, HashSet<long>>();
            nonDetTidToNodeID = new Dictionary<long, long>();

            var node = new ScheduleNode(-1, true);
            node.nextNodeCanExecute.SetResult(true);
            detNodes.Add(-1, node);
            localBidToGlobalBid.Add(-1, -1);
        }

        public void CheckGC()
        {
            if (detNodes.Count > 2) Console.WriteLine($"ScheduleInfo: detNodes.Count = {detNodes.Count}");
            if (localBidToGlobalBid.Count > 2) Console.WriteLine($"ScheduleInfo: localBidToGlobalBid.Count = {localBidToGlobalBid.Count}");
            if (nonDetTidToNodeID.Count != 0) Console.WriteLine($"ScheduleInfo: nonDetTidToNodeID.Count = {nonDetTidToNodeID.Count}");

            if (nonDetNodes.Count > 1) Console.WriteLine($"ScheduleInfo: nonDetNodes.Count = {nonDetNodes.Count}");
            if (nonDetNodeIDToTxnSet.Count > 1) Console.WriteLine($"ScheduleInfo: nonDetNodeIDToTxnSet.Count = {nonDetNodeIDToTxnSet.Count}");
        }

        private ScheduleNode FindTail()
        {
            var node = detNodes[-1];
            while (node.next != null) node = node.next;
            return node;
        }

        public ScheduleNode InsertNonDetTransaction(long tid)
        {
            Debug.Assert(nonDetTidToNodeID.ContainsKey(tid) == false);
            var tail = FindTail();
            if (tail.isDet)
            {
                var node = new ScheduleNode(tid, false);
                var txnSet = new HashSet<long>();
                txnSet.Add(tid);

                nonDetNodeIDToTxnSet.Add(tid, txnSet);
                nonDetNodes.Add(tid, node);
                tail.next = node;
                node.prev = tail;
                tail = node;
            }
            else
            {
                //Join the non-det tail, replace the promise
                if (tail.nextNodeCanExecute.Task.IsCompleted) tail.nextNodeCanExecute = new TaskCompletionSource<bool>();
                nonDetNodeIDToTxnSet[tail.id].Add(tid);
            }
            
            nonDetTidToNodeID.Add(tid, tail.id);
            return tail.prev;
        }

        public void InsertDetBatch(SubBatch subBatch, long globalBid, long highestCommittedBid)
        {
            ScheduleNode node;
            if (!detNodes.ContainsKey(subBatch.bid))
            {
                node = new ScheduleNode(subBatch.bid, true);
                detNodes.Add(subBatch.bid, node);
                localBidToGlobalBid.Add(subBatch.bid, globalBid);
            }
            else node = detNodes[subBatch.bid];

            if (detNodes.ContainsKey(subBatch.lastBid))
            {
                var prevNode = detNodes[subBatch.lastBid];
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
                if (highestCommittedBid >= subBatch.lastBid)
                {
                    var prevNode = detNodes[-1];
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
                    var prevNode = new ScheduleNode(subBatch.lastBid, true);
                    detNodes.Add(subBatch.lastBid, prevNode);
                    localBidToGlobalBid.Add(subBatch.lastBid, subBatch.lastGlobalBid);
                    prevNode.next = node;
                    node.prev = prevNode;
                    Debug.Assert(prevNode.prev == null);
                }
            }
        }

        public ScheduleNode GetDependingNode(long bid)
        {
            return detNodes[bid].prev;
        }

        // <local info, global info>
        public void GetBeforeAfterInfo(long tid, long highestCommittedLocalBid, NonDetScheduleInfo localInfo, NonDetScheduleInfo globalInfo)
        {
            var node = nonDetNodes[nonDetTidToNodeID[tid]];   // get the non-det node
            
            // get max before bid
            var prevNode = node;
            long maxBeforeLocalBid = -1;
            long maxBeforeGlobalBid = -1;
            while (prevNode.prev != null)
            {
                prevNode = prevNode.prev;
                if (prevNode.isDet == false) break;

                var localBid = prevNode.id;
                if (localBid <= highestCommittedLocalBid) break;
                var globalBid = localBidToGlobalBid[localBid];

                maxBeforeLocalBid = Math.Max(maxBeforeLocalBid, localBid);
                maxBeforeGlobalBid = Math.Max(maxBeforeGlobalBid, globalBid);

                if (globalBid != -1) break;
            }

            // get min after bid
            var nextNode = node;
            var minAfterLocalBid = long.MaxValue;
            var minAfterGlobalBid = long.MaxValue;
            while (nextNode.next != null)
            {
                nextNode = nextNode.next;
                if (nextNode.isDet == false) continue;

                var localBid = nextNode.id;
                var globalBid = localBidToGlobalBid[localBid];

                minAfterLocalBid = Math.Min(minAfterLocalBid, localBid);
                minAfterGlobalBid = Math.Min(minAfterGlobalBid, globalBid);

                if (globalBid != -1) break;
            }

            var isLocalAfterComplete = true;
            var isGlobalAfterComplete = true;
            if (minAfterLocalBid == int.MaxValue) isLocalAfterComplete = false;
            if (minAfterGlobalBid == int.MaxValue) isGlobalAfterComplete = false;

            localInfo = new NonDetScheduleInfo(maxBeforeLocalBid, minAfterLocalBid, isLocalAfterComplete);
            globalInfo = new NonDetScheduleInfo(maxBeforeGlobalBid, minAfterGlobalBid, isGlobalAfterComplete);
        }

        public void CompleteDetBatch(long bid)
        {
            detNodes[bid].nextNodeCanExecute.SetResult(true);
        }

        public void CompleteNonDetTxn(long tid)   // when commit/abort a non-det txn
        {
            if (!nonDetTidToNodeID.ContainsKey(tid)) return;
            var nodeId = nonDetTidToNodeID[tid];
            nonDetTidToNodeID.Remove(tid);
            var txnSet = nonDetNodeIDToTxnSet[nodeId];

            txnSet.Remove(tid);
            if (txnSet.Count == 0) nonDetNodes[nodeId].nextNodeCanExecute.SetResult(true);
        }
    }

    public class ScheduleNode
    {
        public long id;
        public bool isDet;
        public TaskCompletionSource<bool> nextNodeCanExecute;
        public ScheduleNode prev;
        public ScheduleNode next;

        public ScheduleNode(long id, bool isDet)
        {
            this.id = id;
            this.isDet = isDet;
            nextNodeCanExecute = new TaskCompletionSource<bool>();
        }
    }
}