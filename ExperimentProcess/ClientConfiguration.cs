using System;
using Orleans;
using Utilities;
using Orleans.Hosting;
using Orleans.Configuration;
using System.Threading.Tasks;
using System.Net;

namespace ExperimentProcess
{
    public static class ClientConfiguration
    {
        readonly static int maxAttempts = 10;
        public static async Task<IClusterClient> StartOrleansClient()
        {
            for (int i = 0; i < maxAttempts; i++)
            {
                try
                {
                    var clientBuilder = new ClientBuilder()
                        .Configure<ClusterOptions>(options =>
                        {
                            options.ClusterId = Constants.ClusterID;
                            options.ServiceId = Constants.ServiceID;
                        });

                    if (Constants.localCluster)
                    {
                        if (Constants.multiSilo)
                        {
                            var gateways = new IPEndPoint[Constants.numSilo + 1];
                            for (int siloID = 0; siloID <= Constants.numSilo; siloID++)
                                gateways[siloID] = new IPEndPoint(IPAddress.Loopback, 30000 + siloID);

                            clientBuilder.UseStaticClustering(gateways);
                        }
                        else clientBuilder.UseLocalhostClustering();
                    }
                    else
                    {
                        Action<DynamoDBGatewayOptions> dynamoDBOptions = options => {
                            options.AccessKey = Constants.AccessKey;
                            options.SecretKey = Constants.SecretKey;
                            options.TableName = Constants.SiloMembershipTable;
                            options.Service = Constants.ServiceRegion;
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
                    Console.WriteLine($"Attemp {i} fails.");
                }
            }

            throw new Exception("Exception: fail to start Orleans Client. ");
        }
    }
}
