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

namespace OrleansSiloHost
{
    public class Program
    {
        static private int siloPort;    // silo-to-silo endpoint
        static private int gatewayPort; // client-to-silo endpoint
        static readonly bool enableOrleansTxn = Constants.implementationType == ImplementationType.ORLEANSTXN ? true : false;

        public static int Main(string[] args)
        {
            if (Constants.multiSilo)
            {
                // var siloID = 0;
                var siloID = int.Parse(args[0]);
                siloPort = 11111 + siloID;
                gatewayPort = 30000 + siloID;
            }
            else
            {
                siloPort = 11111;
                gatewayPort = 30000;
            }
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            try
            {
                var builder = new SiloHostBuilder();
                if (Constants.localCluster == false)
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

                    builder.UseDynamoDBClustering(dynamoDBOptions);
                }
                else builder.UseLocalhostClustering();

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
                        //options.AdvertisedIPAddress = IPAddress.Parse(GetLocalIPAddress());  // IP Address to advertise in the cluster
                    })
                    .ConfigureServices(ConfigureServices)
                    .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

                if (enableOrleansTxn)
                {
                    if (Constants.loggingType == LoggingType.NOLOGGING) builder.AddMemoryTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });
                    else builder.AddFileTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });

                    builder
                        //.Configure<TransactionalStateOptions>(o => o.LockAcquireTimeout = TimeSpan.FromSeconds(20))
                        //.Configure<TransactionalStateOptions>(o => o.LockTimeout = TimeSpan.FromMilliseconds(200))
                        //.Configure<TransactionalStateOptions>(o => o.PrepareTimeout = TimeSpan.FromSeconds(20))
                        .UseTransactions();
                }
                else builder.AddMemoryGrainStorageAsDefault();

                var host = builder.Build();
                await host.StartAsync();

                Console.WriteLine("Press Enter to terminate...");
                Console.ReadLine();
                await host.StopAsync();
                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            // all the singletons have one instance per silo host??

            // dependency injection
            services.AddSingleton<ILoggerGroup, LoggerGroup>();

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

        private static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList) if (ip.AddressFamily == AddressFamily.InterNetwork) return ip.ToString();
            throw new Exception("No network adapters with an IPv4 address in the system!");
        }
    }
}