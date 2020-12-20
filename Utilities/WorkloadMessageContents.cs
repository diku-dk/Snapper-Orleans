using System;
using System.Collections.Generic;

namespace Utilities
{
    public enum BenchmarkType {SMALLBANK, TPCC};
    public enum Distribution { ZIPFIAN, UNIFORM, HOTRECORD }
    public enum ImplementationType { SNAPPER, ORLEANSEVENTUAL, ORLEANSTXN };
   
    public class Constants
    {
        public const bool enableAzureClustering = true;
        public const string connectionString = "DefaultEndpointsProtocol=https;AccountName=silo-membership-table;AccountKey=cyNmVPVYxlTeepACZWayOBtK4yuN5N733nBcaolrVtDjQd8Y04e263oZt8nKWLHNLAVPsMvyU6gO7dHUawmy3A==;TableEndpoint=https://silo-membership-table.table.cosmos.azure.com:443/;";

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
        public List<double> det_latencies;

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

        public void setLatency(List<double> latencies, List<double> det_latencies)
        {
            this.latencies = latencies;
            this.det_latencies = det_latencies;
        }
    }
}
