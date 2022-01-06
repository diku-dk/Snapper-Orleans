using System;
using Orleans;
using Orleans.Runtime;
using Orleans.Hosting;
using Orleans.Providers;
using Microsoft.Extensions.Options;
using Orleans.Transactions.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace OrleansSiloHost
{
    public static class SiloBuilderExtensions
    {
        public static ISiloHostBuilder AddMemoryTransactionalStateStorageAsDefault(this ISiloHostBuilder builder, Action<MyTransactionalStateOptions> configureOptions)
        {
            return builder.AddMemoryTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static ISiloHostBuilder AddMemoryTransactionalStateStorage(this ISiloHostBuilder builder, string name, Action<MyTransactionalStateOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddMemoryTransactionalStateStorage(name, ob => ob.Configure(configureOptions)));
        }

        private static IServiceCollection AddMemoryTransactionalStateStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<MyTransactionalStateOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<MyTransactionalStateOptions>(name));

            services.TryAddSingleton(sp => sp.GetServiceByName<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddSingletonNamedService(name, MemoryTransactionalStateStorageFactory.Create);
            services.AddSingletonNamedService(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<ITransactionalStateStorageFactory>(n));

            return services;
        }

        public static ISiloHostBuilder AddFileTransactionalStateStorageAsDefault(this ISiloHostBuilder builder, Action<MyTransactionalStateOptions> configureOptions)
        {
            return builder.AddFileTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME, configureOptions);
        }

        public static ISiloHostBuilder AddFileTransactionalStateStorage(this ISiloHostBuilder builder, string name, Action<MyTransactionalStateOptions> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddFileTransactionalStateStorage(name, ob => ob.Configure(configureOptions)));
        }

        private static IServiceCollection AddFileTransactionalStateStorage(this IServiceCollection services, string name,
            Action<OptionsBuilder<MyTransactionalStateOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<MyTransactionalStateOptions>(name));

            services.TryAddSingleton(sp => sp.GetServiceByName<ITransactionalStateStorageFactory>(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddSingletonNamedService(name, FileTransactionalStateStorageFactory.Create);
            services.AddSingletonNamedService(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<ITransactionalStateStorageFactory>(n));

            return services;
        }
    }

    public class MyTransactionalStateOptions
    {
        public int InitStage { get; set; }
    }
}