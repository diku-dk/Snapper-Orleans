using System;
using Orleans;
using Utilities;
using System.Net;
using Orleans.Hosting;
using Orleans.Runtime;
using System.Net.Sockets;
using Orleans.Configuration;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using Microsoft.Extensions.DependencyInjection;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Logging;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.Coordinator;
using System.IO;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace SnapperSiloHost
{
    public class Program
    {
        static int siloID = 0;
        static private int siloPort = 11111;    // silo-to-silo endpoint
        static private int gatewayPort = 30000; // client-to-silo endpoint
        static readonly bool enableOrleansTxn = Constants.implementationType == ImplementationType.ORLEANSTXN ? true : false;

        public static int Main(string[] args)
        {
            if (Constants.multiSilo)
            {
                siloID = int.Parse(args[0]);
                siloPort += siloID;
                gatewayPort += siloID;
            }
            return RunMainAsync().Result;
        }

        static async Task<int> RunMainAsync()
        {
            var builder = new SiloHostBuilder();

            if (Constants.LocalCluster == false)
            {
                string ServiceRegion;
                string AccessKey;
                string SecretKey;

                using (var file = new StreamReader(Constants.credentialFile))
                {
                    ServiceRegion = file.ReadLine();
                    AccessKey = file.ReadLine();
                    SecretKey = file.ReadLine();
                }

                Action<DynamoDBClusteringOptions> dynamoDBOptions = options =>
                {
                    options.AccessKey = AccessKey;
                    options.SecretKey = SecretKey;
                    options.TableName = Constants.SiloMembershipTable;
                    options.Service = ServiceRegion;
                    options.WriteCapacityUnits = 10;
                    options.ReadCapacityUnits = 10;
                };

                builder
                    .UseDynamoDBClustering(dynamoDBOptions)
                    .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Parse(GetLocalIPAddress()));
            }
            else
            {
                if (Constants.multiSilo)
                {
                    var primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, 11111);
                    builder.UseDevelopmentClustering(primarySiloEndpoint)
                        // The IP address used for clustering / to be advertised in membership tables
                        .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback);
                }
                else builder.UseLocalhostClustering();
            }

            builder
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Constants.ClusterSilo;
                    options.ServiceId = Constants.ServiceID;
                })
                .Configure<EndpointOptions>(options =>
                {
                    options.SiloPort = siloPort;
                    options.GatewayPort = gatewayPort;
                })
                .ConfigureServices(ConfigureServices);
                //.ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            if (enableOrleansTxn)
            {
                builder.UseTransactions();

                if (Constants.loggingType == LoggingType.NOLOGGING)
                    builder.AddMemoryTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });
                else
                {
                    builder.AddFileTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });

                    builder
                        .Configure<TransactionalStateOptions>(o => o.LockTimeout = TimeSpan.FromMilliseconds(200))
                        .Configure<TransactionalStateOptions>(o => o.LockAcquireTimeout = TimeSpan.FromMilliseconds(200));
                        //.Configure<TransactionalStateOptions>(o => o.PrepareTimeout = TimeSpan.FromSeconds(20));
                }
            }
            else builder.AddMemoryGrainStorageAsDefault();

            var siloHost = builder.Build();
            await siloHost.StartAsync();
            Console.WriteLine("Silo is started...");

            if (Constants.LocalCluster == false && Constants.LocalTest == false)
            {
                if (siloID < Constants.numSilo)
                {
                    // this is the normal silo
                    Debug.Assert(Environment.ProcessorCount >= Constants.numSilo * Constants.numCPUPerSilo);
                    Helper.SetCPU(siloID, "SnapperSiloHost", Constants.numCPUPerSilo);
                }
                else
                {
                    // this is the global coordinator silo
                    Debug.Assert(siloID == Constants.numSilo && Environment.ProcessorCount >= Constants.numCPUForGlobal);
                    Helper.SetCPU(0, "SnapperSiloHost", Constants.numCPUForGlobal);
                }
            }
            
            Console.WriteLine("Press Enter to terminate...");
            Console.ReadLine();
            await siloHost.StopAsync();
            return 0;
        }

        static void ConfigureServices(IServiceCollection services)
        {
            // all the singletons have one instance per silo host??

            // dependency injection
            services.AddSingleton<ILoggerGroup, LoggerGroup>();
            services.AddSingleton<ICoordMap, CoordMap>();

            services.AddSingletonNamedService<PlacementStrategy, GlobalConfigGrainPlacementStrategy>(nameof(GlobalConfigGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, GlobalConfigGrainPlacement>(typeof(GlobalConfigGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, LocalConfigGrainPlacementStrategy>(nameof(LocalConfigGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, LocalConfigGrainPlacement>(typeof(LocalConfigGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, GlobalCoordGrainPlacementStrategy>(nameof(GlobalCoordGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, GlobalCoordGrainPlacement>(typeof(GlobalCoordGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, LocalCoordGrainPlacementStrategy>(nameof(LocalCoordGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, LocalCoordGrainPlacement>(typeof(LocalCoordGrainPlacementStrategy));

            services.AddSingletonNamedService<PlacementStrategy, TransactionExecutionGrainPlacementStrategy>(nameof(TransactionExecutionGrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, TransactionExecutionGrainPlacement>(typeof(TransactionExecutionGrainPlacementStrategy));

        }

        static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList) if (ip.AddressFamily == AddressFamily.InterNetwork) return ip.ToString();
            throw new Exception("No network adapters with an IPv4 address in the system!");
        }
    }
}