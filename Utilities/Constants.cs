﻿namespace Utilities
{
    public enum CCType { S2PL, TS };
    public enum AccessMode { Read, ReadWrite };
    public enum BenchmarkType { SMALLBANK, TPCC };
    public enum Distribution { ZIPFIAN, UNIFORM, HOTRECORD };
    public enum ImplementationType { SNAPPER, ORLEANSEVENTUAL, ORLEANSTXN };
    public enum LoggingType { NOLOGGING, ONGRAIN, LOGGER };
    public enum StorageType { INMEMORY, FILESYSTEM, DYNAMODB };
    public enum TxnType { Init, Balance, MultiTransfer, Deposit };

    public class Constants
    {
        // Client config
        public const int numWorker = 1;

        // architecture 1: single silo
        //                 local coordinators (num = numLocalCoordPerSilo)
        //                 1 global config grain
        // architecture 2: multi silo, non-hierarchical
        //                 all local coordinators locate in a separate silo (num = numGlobalCoord)
        //                 1 global config grain
        //                 1 local config grain per silo
        // architecture 3: multi silo, hierarchical
        //                 in each silo, local coordinators (num = numLocalCoordPerSilo)
        //                 all global coordinators locate in a separate silo (num = numGlobalCoord)
        //                 1 global config grain
        //                 1 local config grain per silo
        public const bool hierarchicalCoord = true;

        // Silo config
        public const int numSilo = 2;
        public const int numCPUBasic = 4;
        public const int numCPUPerSilo = 4;
        public const bool multiSilo = numSilo > 1;
        public const CCType ccType = CCType.S2PL;
        public const ImplementationType implementationType = ImplementationType.SNAPPER;
        public const LoggingType loggingType = LoggingType.NOLOGGING;
        public const StorageType storageType = StorageType.FILESYSTEM;
        public const int numGlobalCoord = numSilo * 2;
        public const int numLocalCoordPerSilo = numCPUPerSilo / numCPUBasic * 8;
        public const int loggingBatchSize = 1;
        public const bool loggingBatching = false;
        public const int numGlobalLogger = 1;
        public const int numLoggerPerSilo = numCPUPerSilo / numCPUBasic * 8;
        // for SmallBank
        public const int numGrainPerSilo = 10 * numCPUPerSilo / numCPUBasic;   // 10000 * ...
        // for TPCC
        public const int NUM_W_PER_SILO = 2 * numCPUPerSilo / numCPUBasic;
        public const int NUM_D_PER_W = 10;
        public const int NUM_C_PER_D = 3000;
        public const int NUM_I = 100000;
        public const int NUM_OrderGrain_PER_D = 1;
        public const int NUM_StockGrain_PER_W = 10000;
        public const int NUM_GRAIN_PER_W = 1 + 1 + 2 * NUM_D_PER_W + NUM_StockGrain_PER_W + NUM_D_PER_W * NUM_OrderGrain_PER_D;

        public const bool enableAzureClustering = false;
        public const string connectionString = "";                               // primary connection string

        public const string TPCC_namespace = "TPCC.Grain.";
        public const string SmallBank_namespace = "SmallBank.Grain.";

        public const string dataPath = @"..\Snapper-Orleans\data\";
        public const string logPath = dataPath + @"log\";
        public const string resultPath = dataPath + "result.txt";
        public const string credentialFile = dataPath + "AWS_credential.txt";

        public const string controller_Local_SinkAddress = "@tcp://localhost:5558";
        public const string controller_Local_WorkerAddress = "@tcp://localhost:5575";
        public const string worker_Local_SinkAddress = ">tcp://localhost:5558";
        public const string worker_Local_ControllerAddress = ">tcp://localhost:5575";

        public const string controller_Remote_SinkAddress = "@tcp://1.1.1.1:5558";    // controller private IP
        public const string controller_Remote_WorkerAddress = "@tcp://*:5575";
        public const string worker_Remote_SinkAddress = ">tcp://1.1.1.1.212:5558";    // controller public IP
        public const string worker_Remote_ControllerAddress = ">tcp://1.1.1.1:5575";  // controller public IP

        public const bool LocalCluster = false;
        public const string LocalSilo = "dev";
        public const string ClusterSilo = "ec2";
        public const string ServiceID = "Snapper";
        public const string LogTable = "SnapperLog";
        public const string GrainStateTable = "SnapperGrainStateTable";
        public const string SiloMembershipTable = "SnapperMembershipTable";

        // for workload generation
        public const int BASE_NUM_MULTITRANSFER = 150000;
        public const int BASE_NUM_NEWORDER = 20000;
    }
}