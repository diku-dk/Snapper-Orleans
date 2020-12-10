using System;

namespace Concurrency.Interface.Nondeterministic
{
    public class DeadlockAvoidanceException : Exception
    {

        public DeadlockAvoidanceException() : base() { }

        public DeadlockAvoidanceException(string message) : base(message) { }
    }
}
