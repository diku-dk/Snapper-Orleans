﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Utilities
{    
    public class NonDeterministicBatchSchedule
    {
        public int id;
        public HashSet<int> transactions;

        public NonDeterministicBatchSchedule(int transactionId)
        {
            int id = transactionId;
            transactions = new HashSet<int>();
            transactions.Add(transactionId);
        }

        public void AddTransaction(int transactionId)
        {
            transactions.Add(transactionId);            
        } 

        public Boolean RemoveTransaction(int transactionId)
        {
            transactions.Remove(transactionId);
            return (transactions.Count == 0);
        }
    }
}
