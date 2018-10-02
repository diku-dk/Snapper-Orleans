using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Hosting;
using Orleans.Hosting.Development;
using Orleans.Configuration;
using System.Net;
using AccountTransfer.Grains;
using Concurrency.Implementation.Deterministic;
using Concurrency.Implementation.Nondeterministic;

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
                    options.ServiceId = "AccountTransferApp";
                    
                })
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(DeterministicTransactionCoordinator).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(NondeterministicTransactionCoordinator).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(ATMGrain).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(AccountGrain).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(OrleansATM).Assembly).WithReferences())
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(OrleansAccount).Assembly).WithReferences())
                .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information))
                .AddMemoryGrainStorageAsDefault()
                .UseInClusterTransactionManager()
                .UseInMemoryTransactionLog()
                .UseTransactionalState();
            
            var host = builder.Build();
            await host.StartAsync();
            return host;
        }
    }
}
