using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Persist.Grains;
using Persist.Interfaces;
using Utilities;
using System.Diagnostics;
using System.Net;
using NetMQ.Sockets;
using NetMQ;
using Microsoft.Extensions.Logging;
using System.IO;

namespace OrleansSnapperSiloHost
{
    public class Program
    {
        static int siloPort = 11111;      // silo-to-silo endpoint
        static int gatewayPort = 30000;   // client-to-silo endpoint
        static int numCPUPerSilo;
        static ImplementationType implementationType;
        static bool enableOrleansTxn;
        static bool loggingEnabled;

        static int Main(string[] args)
        {
            numCPUPerSilo = int.Parse(args[0]);
            implementationType = Enum.Parse<ImplementationType>(args[1]);
            loggingEnabled = bool.Parse(args[2]);
            enableOrleansTxn = implementationType == ImplementationType.ORLEANSTXN;
            return RunMainAsync().Result;
        }

        static async Task<int> RunMainAsync()
        {
            try
            {
                var builder = new SiloHostBuilder();
                if (Constants.LocalCluster == false)
                {
                    string ServiceRegion;
                    string AccessKey;
                    string SecretKey;

                    using (var file = new StreamReader(Constants.credentialFile))
                    {
                        ServiceRegion = file.ReadLine();
                        AccessKey = file.ReadLine();
                        SecretKey = file.ReadLine();
                    }

                    Action<DynamoDBClusteringOptions> dynamoDBOptions = options =>
                    {
                        options.AccessKey = AccessKey;
                        options.SecretKey = SecretKey;
                        options.TableName = Constants.SiloMembershipTable;
                        options.Service = ServiceRegion;
                        options.WriteCapacityUnits = 10;
                        options.ReadCapacityUnits = 10;
                    };

                    builder.UseDynamoDBClustering(dynamoDBOptions);
                }
                else builder.UseLocalhostClustering();

                builder
                    .Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = Constants.ClusterSilo;
                        options.ServiceId = Constants.ServiceID;
                    })
                    .Configure<EndpointOptions>(options =>
                    {
                        options.SiloPort = siloPort;
                        options.GatewayPort = gatewayPort;
                        if (Constants.LocalCluster == false)
                            options.AdvertisedIPAddress = IPAddress.Parse(Helper.GetLocalIPAddress());
                    })
                    .ConfigureServices(ConfigureServices);
                    //.ConfigureLogging(logging => logging.AddConsole().AddFilter("Orleans", LogLevel.Information));

                if (enableOrleansTxn)
                {
                    builder.UseTransactions();

                    if (loggingEnabled)
                    {
                        builder.AddFileTransactionalStateStorageAsDefault(opts =>
                        {
                            opts.InitStage = ServiceLifecycleStage.ApplicationServices;
                            opts.numCPUPerSilo = numCPUPerSilo;
                        });

                        builder
                            .Configure<TransactionalStateOptions>(o => o.LockTimeout = TimeSpan.FromMilliseconds(200))
                            .Configure<TransactionalStateOptions>(o => o.LockAcquireTimeout = TimeSpan.FromMilliseconds(200));
                            //.Configure<TransactionalStateOptions>(o => o.PrepareTimeout = TimeSpan.FromSeconds(20));
                    }
                    else
                    {
                        builder.AddMemoryTransactionalStateStorageAsDefault(opts => 
                        { 
                            opts.InitStage = ServiceLifecycleStage.ApplicationServices;
                            opts.numCPUPerSilo = numCPUPerSilo;
                        });
                    }
                }
                else builder.AddMemoryGrainStorageAsDefault();

                var host = builder.Build();
                await host.StartAsync();
                Console.WriteLine("Silo is started...");

                // ===================================================================================================
                Console.WriteLine("Set processor affinity for SnapperSiloHost...");
                var processes = Process.GetProcessesByName("SnapperSiloHost");
                Debug.Assert(processes.Length == 1);   // there is only one process called "SnapperExperimentProcess"

                var str = Helper.GetSiloProcessorAffinity(numCPUPerSilo);
                var serverProcessorAffinity = Convert.ToInt64(str, 2);     // server uses the lowest n bits
                processes[0].ProcessorAffinity = (IntPtr)serverProcessorAffinity;

                // ===================================================================================================
                var serializer = new MsgPackSerializer();
                string inputSocketAddress;
                string outputSocketAddress;
                if (Constants.LocalCluster)
                {
                    inputSocketAddress = Constants.Silo_LocalCluster_InputSocket;
                    outputSocketAddress = Constants.Silo_LocalCluster_OutputSocket;
                }
                else
                {
                    inputSocketAddress = "@tcp://" + Helper.GetLocalIPAddress() + ":" + Constants.siloInPort;
                    outputSocketAddress = "@tcp://*:" + Constants.siloOutPort;
                }

                var inputSocket = new PullSocket(inputSocketAddress);
                var outputSocket = new PublisherSocket(outputSocketAddress);

                Console.WriteLine("Wait for ExperimentProcess to connect... ");
                var msg = serializer.deserialize<NetworkMessage>(inputSocket.ReceiveFrameBytes());
                Trace.Assert(msg == NetworkMessage.CONNECTED);

                outputSocket.SendFrame(serializer.serialize(NetworkMessage.CONFIRMED));
                Console.WriteLine("Send the confirmation message back to ExperimentProcess. ");

                Console.WriteLine("Wait for ExperimentProcess to finish the experiment... ");
                msg = serializer.deserialize<NetworkMessage>(inputSocket.ReceiveFrameBytes());
                Trace.Assert(msg == NetworkMessage.SIGNAL);
                Console.WriteLine("Receive messgae from ExperimentProcess. Shutting down silo...");
                await host.StopAsync();

                outputSocket.SendFrame(serializer.serialize(NetworkMessage.CONFIRMED));
                Console.WriteLine("Send the confirmation message back to ExperimentProcess. ");

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 1;
            }
        }

        static void ConfigureServices(IServiceCollection services)
        {
            // dependency injection
            services.AddSingleton<IPersistSingletonGroup, PersistSingletonGroup>();
            services.AddSingleton<ITPCCManager, TPCCManager>();
        }
    }
}