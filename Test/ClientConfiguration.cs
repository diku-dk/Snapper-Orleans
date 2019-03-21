using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Orleans;
using Orleans.Runtime;
using Orleans.Configuration;
using Orleans.Hosting;
using Microsoft.Extensions.Logging;
using AccountTransfer.Interfaces;
using AccountTransfer.Grains;
using Concurrency.Implementation;
using System.Net;

namespace Test
{
    class ClientConfiguration
    {        
        private IClusterClient client;
        private static readonly int maxAttempts = 10;
        private ISiloHost host;

        private async Task StartSilo()
        {
            var builder = new SiloHostBuilder()
                .UseLocalhostClustering()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "dev";
                    options.ServiceId = "AccountTransferApp";


                })
                .Configure<EndpointOptions>(options => options.AdvertisedIPAddress = IPAddress.Loopback)
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(GlobalTransactionCoordinator).Assembly).WithReferences())                
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(AccountGrain).Assembly).WithReferences())
                .ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

            host = builder.Build();
            await host.StartAsync();            
        }

        public async Task<IClusterClient> StartClientWithRetries()
        {
            if (client == null)
            {
                await StartSilo();
                int attempt = 0;
                while (true)
                {
                    try
                    {
                        Thread.Sleep(5000);
                        client = new ClientBuilder()
                                    .UseLocalhostClustering()
                                    .Configure<ClusterOptions>(options =>
                                    {
                                        options.ClusterId = "dev";
                                        options.ServiceId = "AccountTransferApp";
                                    })
                                    .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(IAccountGrain).Assembly).WithReferences())
                                    .ConfigureLogging(logging => logging.AddConsole())
                                    .Build();

                        await client.Connect();
                        Console.WriteLine("Client successfully connect to silo host");
                        break;
                    }
                    catch (SiloUnavailableException)
                    {
                        attempt++;
                        Console.WriteLine($"Attempt {attempt} of {maxAttempts} failed to initialize the Orleans client.");
                        if (attempt > maxAttempts)
                        {
                            throw;
                        }
                        await Task.Delay(TimeSpan.FromSeconds(4));
                    }
                }
            }
            return client;
        }
    }
}