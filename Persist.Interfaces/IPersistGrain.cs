﻿using Orleans;
using System.Threading.Tasks;

namespace Persist.Interfaces
{
    public interface IPersistGrain : IGrainWithIntegerKey
    {
        Task Write(byte[] value);
    }
}