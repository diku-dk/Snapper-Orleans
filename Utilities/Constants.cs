namespace Utilities
{
    public enum CCType { S2PL, TS };
    public enum AccessMode { Read, ReadWrite };
    public enum BenchmarkType { SMALLBANK, TPCC };
    public enum Distribution { ZIPFIAN, UNIFORM, HOTRECORD };
    public enum ImplementationType { SNAPPER, ORLEANSEVENTUAL, ORLEANSTXN };
    public enum LoggingType { NOLOGGING, ONGRAIN, PERSISTGRAIN, PERSISTSINGLETON };
    public enum StorageType { INMEMORY, FILESYSTEM, DYNAMODB };
    public enum SerializerType { BINARY, MSGPACK, JSON };

    public class Constants
    {
        // Client config
        public const int numWorker = 1;

        // Silo config
        public const int numSilo = 1;
        public const int numCPUBasic = 4;
        public const int numCPUPerSilo = 4;
        public const bool multiSilo = numSilo > 1;
        public const CCType ccType = CCType.S2PL;
        public const ImplementationType implementationType = ImplementationType.SNAPPER;
        public const LoggingType loggingType = LoggingType.NOLOGGING;
        public const StorageType storageType = StorageType.FILESYSTEM;
        public const SerializerType serializerType = SerializerType.MSGPACK;
        public const int numCoordPerSilo = numCPUPerSilo / numCPUBasic * 8;
        public const int loggingBatchSize = 1;
        public const bool loggingBatching = false;
        public const int numPersistItemPerSilo = numCPUPerSilo / numCPUBasic * 8;
        // for SmallBank
        public const int numGrainPerSilo = 10000 * numCPUPerSilo / numCPUBasic;
        // for TPCC
        public const int NUM_W_PER_SILO = 2 * numCPUPerSilo / numCPUBasic;
        public const int NUM_D_PER_W = 10;
        public const int NUM_C_PER_D = 3000;
        public const int NUM_I = 100000;
        public const int NUM_OrderGrain_PER_D = 1;
        public const int NUM_StockGrain_PER_W = 10000;
        public const int NUM_GRAIN_PER_W = 1 + 1 + 2 * NUM_D_PER_W + NUM_StockGrain_PER_W + NUM_D_PER_W * NUM_OrderGrain_PER_D;

        public const bool enableAzureClustering = false;
        public const string connectionString = "";   // primary connection string

        public const string TPCC_namespace = "TPCC.Grain.";
        public const string SmallBank_namespace = "SmallBank.Grain.";

        public const string logPath = @"D:\log\";
        //public const string logPath = @"C:\Users\Administrator\Desktop\log\";
        public const string dataPath = @"C:\Users\Administrator\Desktop\data\";

        public const string controller_Local_SinkAddress = "@tcp://localhost:5558";
        public const string controller_Local_WorkerAddress = "@tcp://localhost:5575";
        public const string worker_Local_SinkAddress = ">tcp://localhost:5558";
        public const string worker_Local_ControllerAddress = ">tcp://localhost:5575";

        public const string controller_Remote_SinkAddress = "@tcp://1.1.1.1:5558";  // controller private IP
        public const string controller_Remote_WorkerAddress = "@tcp://*:5575";
        public const string worker_Remote_SinkAddress = ">tcp://1.1.1.1.212:5558";    // controller public IP
        public const string worker_Remote_ControllerAddress = ">tcp://1.1.1.1:5575";  // controller public IP

        public const bool localCluster = true;
        public const string LocalSilo = "dev";
        public const string ClusterSilo = "ec2";
        public const string ServiceID = "Snapper";
        public const string LogTable = "SnapperLog";
        public const string ServiceRegion = "";
        public const string AccessKey = "";
        public const string GrainStateTable = "SnapperGrainStateTable";
        public const string SiloMembershipTable = "SnapperMembershipTable";
        public const string SecretKey = "";

        // for workload generation
        public const int BASE_NUM_MULTITRANSFER = 150000;
        public const int BASE_NUM_NEWORDER = 20000;
    }
}