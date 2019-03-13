using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using Orleans;
using Orleans.Runtime;
using Orleans.Configuration;
using Microsoft.Extensions.Logging;
using AccountTransfer.Interfaces;

namespace Test
{
    class ClientConfiguration
    {        
        private IClusterClient client;
        private static readonly int maxAttempts = 10;
        
        public async Task<IClusterClient> StartClientWithRetries()
        {
            if (client == null)
            {
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