using System;
using System.Collections.Generic;
using System.Text;

namespace Concurrency.Interface.Nondeterministic
{
    public class DeadlockAvoidanceException : Exception
    {

        public DeadlockAvoidanceException() : base() { }

        public DeadlockAvoidanceException(String message) : base(message) { }
    }
}
