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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using Concurrency.Implementation.GrainPlacement;
using Concurrency.Interface.Logging;
using Concurrency.Implementation.Logging;
using Concurrency.Interface.Coordinator;
using Concurrency.Implementation.Coordinator;

namespace OrleansSiloHost
{
    public class Program
    {
        static private int siloPort = 11111;    // silo-to-silo endpoint
        static private int gatewayPort = 30000; // client-to-silo endpoint
        static readonly bool enableOrleansTxn = Constants.implementationType == ImplementationType.ORLEANSTXN ? true : false;

        public static int Main(string[] args)
        {
            if (Constants.multiSilo)
            {
                var siloID = int.Parse(args[0]);
                siloPort += siloID;
                gatewayPort += siloID;
            }
            return RunMainAsync().Result;
        }

        static async Task<int> RunMainAsync()
        {
            try
            {
                var siloBuilder = new SiloHostBuilder()
                    .Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = Constants.ClusterID;
                        options.ServiceId = Constants.ServiceID;
                    })
                    .Configure<EndpointOptions>(options =>
                    {
                        options.SiloPort = siloPort;
                        options.GatewayPort = gatewayPort;
                    })
                    .ConfigureServices(ConfigureServices);
                //.ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

                if (Constants.localCluster)
                {
                    if (Constants.multiSilo)
                    {
                        var primarySiloEndpoint = new IPEndPoint(IPAddress.Loopback, 11111);
                        siloBuilder.UseDevelopmentClustering(primarySiloEndpoint)
                            // The IP address used for clustering / to be advertised in membership tables
                            .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback);
                    }
                    else siloBuilder.UseLocalhostClustering();
                }
                else
                {
                    Action<DynamoDBClusteringOptions> dynamoDBOptions = options =>
                    {
                        options.AccessKey = Constants.AccessKey;
                        options.SecretKey = Constants.SecretKey;
                        options.TableName = Constants.SiloMembershipTable;
                        options.Service = Constants.ServiceRegion;
                        options.WriteCapacityUnits = 10;
                        options.ReadCapacityUnits = 10;
                    };

                    siloBuilder
                        .UseDynamoDBClustering(dynamoDBOptions)
                        .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Parse(GetLocalIPAddress()));
                }

                if (enableOrleansTxn)
                {
                    if (Constants.loggingType == LoggingType.NOLOGGING)
                        siloBuilder.AddMemoryTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });
                    else siloBuilder.AddFileTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });

                    siloBuilder
                        //.Configure<TransactionalStateOptions>(o => o.LockAcquireTimeout = TimeSpan.FromSeconds(20))
                        //.Configure<TransactionalStateOptions>(o => o.LockTimeout = TimeSpan.FromMilliseconds(200))
                        //.Configure<TransactionalStateOptions>(o => o.PrepareTimeout = TimeSpan.FromSeconds(20))
                        .UseTransactions();
                }
                else siloBuilder.AddMemoryGrainStorageAsDefault();

                var siloHost = siloBuilder.Build();
                await siloHost.StartAsync();

                Console.WriteLine("Press Enter to terminate...");
                Console.ReadLine();
                await siloHost.StopAsync();
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
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