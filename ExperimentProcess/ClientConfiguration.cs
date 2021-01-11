using System;
using Orleans;
using Utilities;
using Orleans.Runtime;
using Orleans.Hosting;
using System.Threading;
using Orleans.Configuration;
using System.Threading.Tasks;
using Orleans.Clustering.AzureStorage;

namespace ExperimentProcess
{
    public class ClientConfiguration
    {
        private IClusterClient client;
        private static readonly int maxAttempts = 10;

        public async Task<IClusterClient> StartClientWithRetriesToCluster(int initializeAttemptsBeforeFailing = 5)
        {
            int attempt = 0;
            IClusterClient client;
            while (true)
            {
                try
                {
                    Action<DynamoDBGatewayOptions> dynamoDBOptions = options => {
                        options.AccessKey = Constants.AccessKey;
                        options.SecretKey = Constants.SecretKey;
                        options.TableName = Constants.SiloMembershipTable;
                        options.Service = Constants.ServiceRegion;
                        options.WriteCapacityUnits = 10;
                        options.ReadCapacityUnits = 10;

                    };

                    Action<AzureStorageGatewayOptions> azureOptions = azureOptions => {
                        azureOptions.ConnectionString = Constants.connectionString;
                    };

                    var clientBuilder = new ClientBuilder()
                        .Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = Constants.ClusterSilo;
                            options.ServiceId = Constants.ServiceID;
                        });

                    if (Constants.enableAzureClustering) clientBuilder.UseAzureStorageClustering(azureOptions);
                    else clientBuilder.UseDynamoDBClustering(dynamoDBOptions);

                    client = clientBuilder.Build();
                    await client.Connect();
                    Console.WriteLine("Client successfully connect to silo host");
                    break;
                }
                catch (SiloUnavailableException)
                {
                    attempt++;
                    Console.WriteLine($"Attempt {attempt} of {initializeAttemptsBeforeFailing} failed to initialize the Orleans client.");
                    if (attempt > initializeAttemptsBeforeFailing)
                    {
                        throw;
                    }
                    await Task.Delay(TimeSpan.FromSeconds(4));
                }
            }

            return client;
        }

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
                                        options.ClusterId = Constants.LocalSilo;
                                        options.ServiceId = Constants.ServiceID;
                                    })
                                    //.ConfigureLogging(logging => logging.AddConsole())
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
