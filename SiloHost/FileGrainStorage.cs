using System;
using Orleans;
using System.IO;
using Utilities;
using Orleans.Storage;
using Orleans.Runtime;
using Orleans.Hosting;
using System.Threading;
using Orleans.Configuration;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Orleans.Configuration.Overrides;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Providers;
using Orleans.Transactions;
using System.Collections.Generic;
using Orleans.Transactions.Abstractions;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace OrleansSiloHost
{
    public static class FileSiloBuilderExtensions
    {
        public static ISiloHostBuilder AddFileGrainStorage(this ISiloHostBuilder builder, string providerName, Action<FileGrainStorageOptions> options)
        {
            return builder.ConfigureServices(services => services.AddFileGrainStorage(providerName, options));
        }

        public static IServiceCollection AddFileGrainStorage(this IServiceCollection services, string providerName, Action<FileGrainStorageOptions> options)
        {
            services.AddOptions<FileGrainStorageOptions>(providerName).Configure(options);
            return services
                .AddSingletonNamedService(providerName, FileGrainStorageFactory.Create)
                .AddSingletonNamedService(providerName, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainStorage>(n));
        }
    }

    public static class FileGrainStorageFactory
    {
        internal static IGrainStorage Create(IServiceProvider services, string name)
        {
            IOptionsSnapshot<FileGrainStorageOptions> optionsSnapshot = services.GetRequiredService<IOptionsSnapshot<FileGrainStorageOptions>>();
            return ActivatorUtilities.CreateInstance<FileGrainStorage>(services, name, optionsSnapshot.Get(name), services.GetProviderClusterOptions(name));
        }
    }

    public class FileGrainStorageOptions
    {
        public string RootDirectory { get; set; }
    }

    public class FileGrainStorage : IGrainStorage, ILifecycleParticipant<ISiloLifecycle>
    {
        private ISerializer serializer = new MsgPackSerializer();
        private readonly string _storageName;
        private readonly FileGrainStorageOptions _options;
        private readonly ClusterOptions _clusterOptions;
        private readonly IGrainFactory _grainFactory;
        private readonly ITypeResolver _typeResolver;

        public FileGrainStorage(string storageName, FileGrainStorageOptions options, IOptions<ClusterOptions> clusterOptions, IGrainFactory grainFactory, ITypeResolver typeResolver)
        {
            _storageName = storageName;
            _options = options;
            _clusterOptions = clusterOptions.Value;
            _grainFactory = grainFactory;
            _typeResolver = typeResolver;
        }


        public Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var fName = GetKeyString(grainType, grainReference);
            var path = Path.Combine(_options.RootDirectory, fName);

            var fileInfo = new FileInfo(path);
            if (fileInfo.Exists)
            {
                if (fileInfo.LastWriteTimeUtc.ToString() != grainState.ETag)
                {
                    throw new InconsistentStateException($"Version conflict (ClearState): ServiceId={_clusterOptions.ServiceId} ProviderName={_storageName} GrainType={grainType} GrainReference={grainReference.ToKeyString()}.");
                }

                grainState.ETag = null;
                grainState.State = Activator.CreateInstance(grainState.State.GetType());
                fileInfo.Delete();
            }

            return Task.CompletedTask;
        }

        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            throw new NotImplementedException();
        }

        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            var storedData = serializer.serialize(grainState.State);

            var fName = GetKeyString(grainType, grainReference);
            var path = Path.Combine(_options.RootDirectory, fName);

            var fileInfo = new FileInfo(path);

            if (fileInfo.Exists && fileInfo.LastWriteTimeUtc.ToString() != grainState.ETag)
            {
                throw new InconsistentStateException($"Version conflict (WriteState): ServiceId={_clusterOptions.ServiceId} ProviderName={_storageName} GrainType={grainType} GrainReference={grainReference.ToKeyString()}.");
            }

            using (var file = new FileStream(path, FileMode.Create, FileAccess.Write))
            {
                await file.WriteAsync(storedData, 0, storedData.Length);
            }

            fileInfo.Refresh();
            grainState.ETag = fileInfo.LastWriteTimeUtc.ToString();
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(OptionFormattingUtilities.Name<FileGrainStorage>(_storageName), ServiceLifecycleStage.ApplicationServices, Init);
        }

        private Task Init(CancellationToken ct)
        {
            return Task.CompletedTask;
        }

        private string GetKeyString(string grainType, GrainReference grainReference)
        {
            return $"{_clusterOptions.ServiceId}.{grainReference.ToKeyString()}.{grainType}";
        }
    }
}
