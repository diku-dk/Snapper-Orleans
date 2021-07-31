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
        // client config
        public const int numWorker = 1;
        public const bool multiWorker = numWorker > 1;
        public const int numEpoch = 6;
        public const int numWarmupEpoch = 2;
        public const int epochDurationMSecs = 8000;
        public const int numThreadsPerWorkerNode = 1;
        public const int numConnToClusterPerWorkerNode = 1;
        public static readonly int[] percentilesToCalculate = { 25, 50, 75, 90, 99 };

        // for workload generation
        public const double txnSkewness = 0.75;     // if txnsize = 4, 3 of grains are from hot set
        public const double grainSkewness = 0.01;   // 1% grain are hot grain
        public const int BASE_NUM_MULTITRANSFER = 150000;
        public const int BASE_NUM_NEWORDER = 20000;

        // silo config
        public const int numSilo = 2;
        public const int numCPUPerSilo = 4;
        public const bool multiSilo = numSilo > 1;
        // for snapper
        public const int numCoordPerSilo = numCPUPerSilo * 2;
        public const int numPersistItemPerSilo = numCPUPerSilo * 2;
        public const int numCoord = numCoordPerSilo * numSilo;
        // for SmallBank
        public const int numGrainPerSilo = 10000 * numCPUPerSilo / 4;
        public const int numGrain = numGrainPerSilo * numSilo;
        // for TPCC
        public const int NUM_W_PER_SILO = numCPUPerSilo / 2;
        public const int NUM_D_PER_W = 10;
        public const int NUM_C_PER_D = 3000;
        public const int NUM_I = 100000;
        public const int NUM_OrderGrain_PER_D = 1;
        public const int NUM_StockGrain_PER_W = 10000;
        public const int NUM_GRAIN_PER_W = 1 + 1 + 2 * NUM_D_PER_W + NUM_StockGrain_PER_W + NUM_D_PER_W * NUM_OrderGrain_PER_D;

        public const bool enableAzureClustering = false;
        public const string connectionString = "DefaultEndpointsProtocol=https;AccountName=silo-membership-table;AccountKey=cyNmVPVYxlTeepACZWayOBtK4yuN5N733nBcaolrVtDjQd8Y04e263oZt8nKWLHNLAVPsMvyU6gO7dHUawmy3A==;TableEndpoint=https://silo-membership-table.table.cosmos.azure.com:443/;";   // primary connection string

        public const string TPCC_namespace = "TPCC.Grain.";
        public const string SmallBank_namespace = "SmallBank.Grain.";

        public const string logPath = @"D:\log\";
        //public const string logPath = @"C:\Users\Administrator\Desktop\log\";
        public const string dataPath = @"C:\Users\Administrator\Desktop\data\";
        //public const string dataPath = @"C:\Users\Yijian\Desktop\data\";    // only used for Azure

        public const string controller_Local_SinkAddress = "@tcp://localhost:5558";
        public const string controller_Local_WorkerAddress = "@tcp://localhost:5575";
        public const string worker_Local_SinkAddress = ">tcp://localhost:5558";
        public const string worker_Local_ControllerAddress = ">tcp://localhost:5575";

        public const string controller_Remote_SinkAddress = "@tcp://172.31.12.68:5558";  // controller private IP
        public const string controller_Remote_WorkerAddress = "@tcp://*:5575";
        public const string worker_Remote_SinkAddress = ">tcp://18.221.111.212:5558";    // controller public IP
        public const string worker_Remote_ControllerAddress = ">tcp://18.221.111.212:5575";  // controller public IP

        public const bool localCluster = false;
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
}