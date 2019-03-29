using System;
using System.Collections.Generic;
using System.Text;

namespace Utilities
{
    public class Optional<T>
    {
        T value;
        bool set = false;

        public Optional(T value)
        {
            this.value = value;
            this.set = true;
        }

        public Optional()
        {
            set = false;
        }

        public bool isSet()
        {
            return set;
        }

        public T getValue()
        {
            if(!set)
            {
                throw new FieldAccessException("Value has not been set!!");
            }
            return value;
        }
    }
}
