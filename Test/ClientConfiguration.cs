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
using SmallBank.Interfaces;
using SmallBank.Grains;

namespace Test
{
    class ClientConfiguration
    {        
        private IClusterClient client;
        private static readonly int maxAttempts = 10;
        private ISiloHost host;

        public async Task<IClusterClient> StartClientWithRetries()
        {
            if (client == null)
            {
                //await StartSilo();
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