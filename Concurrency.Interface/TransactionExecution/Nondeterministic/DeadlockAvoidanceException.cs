using System;

namespace Concurrency.Interface.TransactionExecution.Nondeterministic
{
    public class DeadlockAvoidanceException : Exception
    {
        public DeadlockAvoidanceException() : base() { }
        public DeadlockAvoidanceException(string message) : base(message) { }
    }
}
