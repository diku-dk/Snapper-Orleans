using System;

namespace Utilities
{
    public enum CCType { S2PL, TS };
    public enum AccessMode { Read, ReadWrite };
    public enum BenchmarkType { SMALLBANK, TPCC, NEWSMALLBANK };
    public enum Distribution { UNIFORM, ZIPFIAN, HOTSPOT, NOCONFLICT };
    public enum ImplementationType { SNAPPER, NONTXN, ORLEANSTXN };
    public enum AllTxnTypes { Init, GetBalance, MultiTransfer, Deposit, MultiTransferNoDeadlock, MultiTransferWithNOOP, DepositWithNOOP};
    public enum LoggingType { ONGRAIN, PERSISTGRAIN, PERSISTSINGLETON };
    public enum StorageType { INMEMORY, FILESYSTEM, DYNAMODB };
    public enum SerializerType { BINARY, MSGPACK, JSON };

    [Serializable]
    public enum NetworkMessage { CONNECTED, SIGNAL, CONFIRMED };

    public class Constants
    {
        // Silo config
        public const int numCPUBasic = 4;
        public const CCType ccType = CCType.S2PL;
        public const LoggingType loggingType = LoggingType.PERSISTSINGLETON;
        public const StorageType storageType = StorageType.FILESYSTEM;
        public const SerializerType serializerType = SerializerType.MSGPACK;
        public const int loggingBatchSize = 1;
        public const bool loggingBatching = false;
        // for general benchmark
        public const int numEpoch = 6;
        public const int numWarmupEpoch = 2;
        public const int epochDurationMSecs = 10000;
        public const int numThreadsPerWorkerNode = 1;
        public const int numConnToClusterPerWorkerNode = 1;
        public static int[] percentilesToCalculate = { 50, 90, 99 };
        // for SmallBank
        public const double txnSkewness = 0.75;
        public const double grainSkewness = 0.01;
        // for TPCC
        public const int NUM_D_PER_W = 10;
        public const int NUM_C_PER_D = 3000;
        public const int NUM_I = 100000;
        public const int NUM_StockGrain_PER_W = 10000;

        public const string TPCC_namespace = "TPCC.Grain.";
        public const string SmallBank_namespace = "SmallBank.Grain.";

        public const string dataPath = @"..\Snapper-Sigmod-2022\data\";
        public const string logPath = dataPath + @"log\";
        public const string resultPath = dataPath + "result.txt";
        public const string latencyPath = dataPath + "breakdown_latency.txt";
        public const string credentialFile = @"..\Snapper-Sigmod-2022\AWS_credential.txt";

        public const bool LocalCluster = true;
        public const string LocalSilo = "dev";
        public const string ClusterSilo = "ec2";
        public const string ServiceID = "Snapper";
        public const string LogTable = "SnapperLog";
        public const string GrainStateTable = "SnapperGrainStateTable";
        public const string SiloMembershipTable = "SnapperMembershipTable";

        // for connection between ExperimentProcess and Silo
        public const string siloOutPort = "5558";
        public const string siloInPort = "5575";
        public const string Silo_LocalCluster_InputSocket = "@tcp://localhost:" + Constants.siloInPort;
        public const string Silo_LocalCluster_OutputSocket = "@tcp://localhost:" + Constants.siloOutPort;
        public const string Worker_LocalCluster_InputSocket = ">tcp://localhost:" + Constants.siloOutPort;
        public const string Worker_LocalCluster_OutputSocket = ">tcp://localhost:" + Constants.siloInPort;

        public const int numIntervals = 9;
    }
}