using System;
using Orleans;
using Utilities;
using System.Net;
using Orleans.Hosting;
using Orleans.Runtime;
using Orleans.Configuration;
using System.Threading.Tasks;
using Orleans.Runtime.Placement;
using Concurrency.Implementation;
using Microsoft.Extensions.Logging;
using Orleans.Clustering.AzureStorage;
using Microsoft.Extensions.DependencyInjection;

namespace OrleansSiloHost
{
    public class Program
    {
        static private int siloPort;
        static private int gatewayPort;
        static readonly bool enableOrleansTxn = false;

        public static int Main(string[] args)
        {
            if (Constants.multiSilo)
            {
                siloPort = int.Parse(args[0]);
                gatewayPort = int.Parse(args[1]);
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
                ISiloHost host;
                if (Constants.localCluster) host = await StartSilo();
                else host = await StartClusterSilo();
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

        private static async Task<ISiloHost> StartSilo()
        {

            var builder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Constants.LocalSilo;
                    options.ServiceId = Constants.ServiceID;
                })
                .Configure<GrainCollectionOptions>(options =>
                {
                    // Set the value of CollectionAge to 10 minutes for all grain
                    options.CollectionAge = TimeSpan.FromMinutes(1000);
                })
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback);
            //.ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            if (enableOrleansTxn)
                builder
                    .AddMemoryTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; })
                    .UseTransactions();
            else builder.AddMemoryGrainStorageAsDefault();

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }

        private static async Task<ISiloHost> StartClusterSilo()
        {
            Action<DynamoDBClusteringOptions> dynamoDBOptions = options => {
                options.AccessKey = Constants.AccessKey;
                options.SecretKey = Constants.SecretKey;
                options.TableName = Constants.SiloMembershipTable;
                options.Service = Constants.ServiceRegion;
                options.WriteCapacityUnits = 10;
                options.ReadCapacityUnits = 10;
            };

            Action<AzureStorageClusteringOptions> azureOptions = azureOptions => {
                azureOptions.ConnectionString = Constants.connectionString;
            };

            var builder = new SiloHostBuilder()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = Constants.ClusterSilo;
                    options.ServiceId = Constants.ServiceID;
                })
                .Configure<GrainCollectionOptions>(options =>
                {
                    // Set the value of CollectionAge to 1000 minutes for all grain
                    options.CollectionAge = TimeSpan.FromMinutes(1000);
                })
                .ConfigureEndpoints(siloPort: siloPort, gatewayPort: gatewayPort)
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Parse(Helper.GetLocalIPAddress()))
                .ConfigureServices(ConfigureServices);
            //.ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            if (enableOrleansTxn)
            {
                if (Constants.enableAzureClustering)
                {
                    builder
                        .AddAzureTableTransactionalStateStorage("TransactionStore", options =>
                        {
                            options.ConnectionString = Constants.connectionString;
                        });
                }
                else
                    builder.AddMemoryTransactionalStateStorageAsDefault(opts => { opts.InitStage = ServiceLifecycleStage.ApplicationServices; });
                builder.UseTransactions();
            }
            else builder.AddMemoryGrainStorageAsDefault();

            if (Constants.enableAzureClustering) builder.UseAzureStorageClustering(azureOptions);
            else builder.UseDynamoDBClustering(dynamoDBOptions);

            var host = builder.Build();
            await host.StartAsync();
            return host;
        }

        private static void ConfigureServices(IServiceCollection services)
        {
            services.AddSingletonNamedService<PlacementStrategy, CoordPlacementStrategy>(nameof(CoordPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, CoordPlacement>(typeof(CoordPlacementStrategy));
            services.AddSingletonNamedService<PlacementStrategy, GrainPlacementStrategy>(nameof(GrainPlacementStrategy));
            services.AddSingletonKeyedService<Type, IPlacementDirector, GrainPlacement>(typeof(GrainPlacementStrategy));
        }
    }
}
