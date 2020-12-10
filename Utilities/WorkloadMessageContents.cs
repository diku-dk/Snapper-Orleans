using System;
using System.Collections.Generic;

namespace Utilities
{
    public enum BenchmarkType {SMALLBANK, TPCC};
    public enum Distribution { ZIPFIAN, UNIFORM, HOTRECORD }
    public enum ImplementationType { SNAPPER, ORLEANSEVENTUAL, ORLEANSTXN };
   
    public class Constants
    {
        public const string logPath = @"D:\log\";
        public const string dataPath = @"C:\Users\Administrator\Desktop\data\";

        public const bool multiWorker = false;
        public const string controller_Local_SinkAddress = "@tcp://localhost:5558";
        public const string controller_Local_WorkerAddress = "@tcp://localhost:5575";
        public const string worker_Local_SinkAddress = ">tcp://localhost:5558";
        public const string worker_Local_ControllerAddress = ">tcp://localhost:5575";

        public const string controller_Remote_SinkAddress = "@tcp://172.31.12.68:5558";  // controller private IP
        public const string controller_Remote_WorkerAddress = "@tcp://*:5575";
        public const string worker_Remote_SinkAddress = ">tcp://18.188.44.200:5558";    // controller public IP
        public const string worker_Remote_ControllerAddress = ">tcp://18.188.44.200:5558";  // controller public IP

        public const bool multiSilo = false;
        public const bool localCluster = false;
        public const int numCoordPerSilo = 8;
        public const string LocalSilo = "dev";
        public const string ClusterSilo = "ec2";
        public const string ServiceID = "Snapper";
        public const string LogTable = "SnapperLog";
        public const string ServiceRegion = "us-east-2";
        public const string AccessKey = "AKIAQHVFG6FCI24D3EFV";
        public const string GrainStateTable = "SnapperGrainStateTable";
        public const string SiloMembershipTable = "SnapperMembershipTable";
        public const string SecretKey = "4ZqPYtEtNxht7PwJGySzVqTJtSYmfuGcuVuy3Dsk";
    }

    [Serializable]
    public class WorkloadConfiguration
    {
        public int numConnToClusterPerWorkerNode;
        public int numWorkerNodes;
        public int numThreadsPerWorkerNode;
        public int asyncMsgLengthPerThread;
        public BenchmarkType benchmark;
        public int numEpochs;
        public int epochDurationMSecs;
        public Distribution distribution;
        //SmallBank Specific configurations
        public int numAccounts;
        public int numAccountsPerGroup;
        public int[] mixture;//{getBalance, depositChecking, transder, transacSaving, writeCheck, multiTransfer}
        public int numAccountsMultiTransfer;
        public int numGrainsMultiTransfer;
        public float zipfianConstant;
        public float deterministicTxnPercent;
        public ImplementationType grainImplementationType;
        public int[] percentilesToCalculate;
    }

    [Serializable]
    public class WorkloadResults
    {
        public int numDeadlock;
        public int numNotSerializable;
        public int numDetCommitted;
        public int numNonDetCommitted;
        public int numDetTxn;
        public int numNonDetTxn;
        public long startTime;
        public long endTime;
        public List<double> latencies;
        public List<double> networkTime;
        public List<double> emitTime;
        public List<double> executeTime;
        public List<double> waitBatchCommitTime;

        public List<double> det_latencies;
        public List<double> det_networkTime;
        public List<double> det_emitTime;
        public List<double> det_waitBatchScheduleTime;
        public List<double> det_executeTime;
        public List<double> det_waitBatchCommitTime;

        public WorkloadResults(int numDetTxn, int numNonDetTxn, int numDetCommitted, int numNonDetCommitted, long startTime, long endTime, int numNotSerializable, int numDeadlock)
        {
            this.numDetTxn = numDetTxn;
            this.numNonDetTxn = numNonDetTxn;
            this.numDetCommitted = numDetCommitted;
            this.numNonDetCommitted = numNonDetCommitted;
            this.startTime = startTime;
            this.endTime = endTime;
            this.numNotSerializable = numNotSerializable;
            this.numDeadlock = numDeadlock;
        }

        public void setTime(List<double> latencies, List<double> networkTime, List<double> emitTime, List<double> executeTime, List<double> waitBatchCommitTime)
        {
            this.latencies = latencies;
            this.networkTime = networkTime;
            this.emitTime = emitTime;
            this.executeTime = executeTime;
            this.waitBatchCommitTime = waitBatchCommitTime;
        }

        public void setDetTime(List<double> latencies, List<double> networkTime, List<double> emitTime, List<double> waitBatchScheduleTime, List<double> executeTime, List<double> waitBatchCommitTime)
        {
            det_latencies = latencies;
            det_networkTime = networkTime;
            det_emitTime = emitTime;
            det_waitBatchScheduleTime = waitBatchScheduleTime;
            det_executeTime = executeTime;
            det_waitBatchCommitTime = waitBatchCommitTime;
        }
    }
}
