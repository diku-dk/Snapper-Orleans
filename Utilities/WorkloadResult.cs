using System;
using System.Collections.Generic;

namespace Utilities
{
    [Serializable]
    public class WorkloadResult
    {
        public readonly int numDeadlock;
        public readonly int numNotSerializable;
        public readonly int numNotSureSerializable;
        public readonly int numDetCommitted;
        public readonly int numNonDetCommitted;
        public readonly int numDetTxn;
        public readonly int numNonDetTxn;
        public readonly long startTime;
        public readonly long endTime;
        public List<double> latencies;
        public List<double> det_latencies;

        public List<double>[] breakdown;

        public WorkloadResult(int numDetTxn, int numNonDetTxn, int numDetCommitted, int numNonDetCommitted, long startTime, long endTime, int numNotSerializable, int numNotSureSerializable, int numDeadlock)
        {
            this.numDetTxn = numDetTxn;
            this.numNonDetTxn = numNonDetTxn;
            this.numDetCommitted = numDetCommitted;
            this.numNonDetCommitted = numNonDetCommitted;
            this.startTime = startTime;
            this.endTime = endTime;
            this.numNotSerializable = numNotSerializable;
            this.numNotSureSerializable = numNotSureSerializable;
            this.numDeadlock = numDeadlock;
        }

        public void setLatency(List<double> latencies, List<double> det_latencies)
        {
            this.latencies = latencies;
            this.det_latencies = det_latencies;
        }

        public void setBreakdownLatency(List<double>[] breakdown)
        {
            this.breakdown = breakdown;
        }
    }
}