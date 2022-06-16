using System;
using Orleans;
using Utilities;
using Orleans.Hosting;
using Orleans.Configuration;
using System.Threading.Tasks;
using System.Net;
using System.IO;
using System.Threading;

namespace SnapperExperimentProcess
{
    public class OrleansClientManager
    {
        readonly int maxAttempt = 5;
        readonly string ServiceRegion;
        readonly string AccessKey;
        readonly string SecretKey;

        readonly Random rnd = new Random();

        public OrleansClientManager()
        {
            using (var file = new StreamReader(Constants.credentialFile))
            {
                ServiceRegion = file.ReadLine();
                AccessKey = file.ReadLine();
                SecretKey = file.ReadLine();
            }
        }

        public async Task<IClusterClient> StartOrleansClient()
        {
            for (int i = 0; i < maxAttempt; i++)
            {
                try
                {
                    var clientBuilder = new ClientBuilder()
                        .Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = Constants.ClusterSilo;
                            options.ServiceId = Constants.ServiceID;
                        });

                    if (Constants.LocalCluster)
                    {
                        if (Constants.multiSilo)
                        {
                            int totalNumSilo = Constants.numSilo;
                            if (Constants.multiSilo && Constants.hierarchicalCoord) totalNumSilo++;
                            var gateways = new IPEndPoint[totalNumSilo];
                            for (int siloID = 0; siloID < totalNumSilo; siloID++)
                                gateways[siloID] = new IPEndPoint(IPAddress.Loopback, 30000 + siloID);

                            clientBuilder.UseStaticClustering(gateways);
                        }
                        else clientBuilder.UseLocalhostClustering();
                    }
                    else
                    {
                        Action<DynamoDBGatewayOptions> dynamoDBOptions = options => {
                            options.AccessKey = AccessKey;
                            options.SecretKey = SecretKey;
                            options.TableName = Constants.SiloMembershipTable;
                            options.Service = ServiceRegion;
                            options.WriteCapacityUnits = 10;
                            options.ReadCapacityUnits = 10;

                        };

                        clientBuilder.UseDynamoDBClustering(dynamoDBOptions);
                    }

                    var client = clientBuilder.Build();
                    await client.Connect();
                    Console.WriteLine("Client successfully connect to silo host");
                    return client;
                }
                catch (Exception)
                {
                    Console.WriteLine($"Attempt {i} failed to initialize the Orleans client.");
                    var time = rnd.Next(0, 10);
                    Thread.Sleep(time * 1000);
                }
            }
            throw new Exception($"Fail to create OrleansClient");
        }
    }
}
