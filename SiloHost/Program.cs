using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Configuration;
using System.Net;
using Utilities;

namespace OrleansSiloHost
{
    public class Program
    {
        static readonly bool localCluster = false;
        static readonly bool enableOrleansTxn = false;
        public static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            try
            {
                ISiloHost host;
                if (localCluster) 
                    host = await StartSilo();
                else
                    host = await StartClusterSilo();
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
                    options.ClusterId = "dev";
                    options.ServiceId = "Snapper";
                })
                .Configure<GrainCollectionOptions>(options =>
                {
                    // Set the value of CollectionAge to 10 minutes for all grain
                    options.CollectionAge = TimeSpan.FromMinutes(1000);
                    // Override the value of CollectionAge to 5 minutes for MyGrainImplementation
                    //options.ClassSpecificCollectionAge[typeof(MyGrainImplementation).FullName] = TimeSpan.FromMinutes(5);
                })
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                //.Configure<SchedulingOptions>(o => o.MaxActiveThreads = 1)
                .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            if(enableOrleansTxn)
                builder.AddMemoryGrainStorageAsDefault().UseTransactions();
            
            var host = builder.Build();
            await host.StartAsync();
            return host;
        }

        private static async Task<ISiloHost> StartClusterSilo()
        {

            Action<DynamoDBClusteringOptions> dynamoDBOptions = options => {
                options.AccessKey = "AKIAJILO2SVPTNUZB55Q";
                options.SecretKey = "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";
                options.TableName = "SnapperMembershipTable";
                options.Service = "eu-west-1";
                options.WriteCapacityUnits = 10;
                options.ReadCapacityUnits = 10;

            };

            var builder = new SiloHostBuilder()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "ec2";
                    options.ServiceId = "Snapper";
                })
                .Configure<GrainCollectionOptions>(options =>
                {
                    // Set the value of CollectionAge to 1000 minutes for all grain
                    options.CollectionAge = TimeSpan.FromMinutes(1000);
                    // Override the value of CollectionAge to 5 minutes for MyGrainImplementation
                    //options.ClassSpecificCollectionAge[typeof(MyGrainImplementation).FullName] = TimeSpan.FromMinutes(5);
                })
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Parse(Helper.GetLocalIPAddress()))
                //.Configure<SchedulingOptions>(o => o.MaxActiveThreads = 32)
                .UseDynamoDBClustering(dynamoDBOptions)
                .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            if (enableOrleansTxn)
                builder.AddMemoryGrainStorageAsDefault().UseTransactions();

            var host = builder.Build();            
            await host.StartAsync();
            return host;
        }
    }
}
