using System;
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
                        options.AccessKey = "AKIAJILO2SVPTNUZB55Q";
                        options.SecretKey = "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";
                        options.TableName = "SnapperMembershipTable";
                        options.Service = "eu-west-1";
                        options.WriteCapacityUnits = 10;
                        options.ReadCapacityUnits = 10;

                    };

                    client = new ClientBuilder()
                        .Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = "ec2";
                            options.ServiceId = "Snapper";
                        })
                        .UseDynamoDBClustering(dynamoDBOptions)
                        //.ConfigureLogging(logging => logging.AddConsole())
                        .Build();

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
                                        options.ClusterId = "dev";
                                        options.ServiceId = "Snapper";
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
