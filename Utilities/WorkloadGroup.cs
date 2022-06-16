namespace Utilities
{
    public class WorkloadGroup
    {
        public int[] pactPercent;

        public int[] txnSize;
        public int[] numWriter;

        public Distribution[] distribution;
        public double[] zipfianConstant;
        public int[] actPipeSize;
        public int[] pactPipeSize;

        public bool[] noDeadlock;     // only used for OrleansTxn
    }

    public class Workload
    {
        public int pactPercent;

        public int txnSize;
        public int numWriter;

        public Distribution distribution;
        public double zipfianConstant;
        public int actPipeSize;
        public int pactPipeSize;

        public bool noDeadlock = false;     // only used for OrleansTxn
    }
}
