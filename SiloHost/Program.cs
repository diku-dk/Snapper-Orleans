using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Configuration;
using System.Net;
using AccountTransfer.Grains;
using Utilities;
using SmallBank.Grains;
using SmallBank.Interfaces;
using AccountTransfer.Interfaces;

namespace OrleansSiloHost
{
    public class Program
    {
        public static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {
            try
            {
                var host = await StartSilo();
                //var host = await StartClusterSilo();
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
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(CustomerAccountGroupGrain).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(AccountGrain).Assembly).WithReferences())
                .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));
            
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
                .UseDynamoDBClustering(dynamoDBOptions)
                .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            var host = builder.Build();
            
            await host.StartAsync();
            return host;
        }
    }
}
