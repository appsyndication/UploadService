using System;
using System.IO;
using System.Threading.Tasks;
using AppSyndication.BackendModel.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.DependencyInjection;

namespace AppSyndication.WebJobs.IndexChannelJob
{
    public static class Program
    {
        public static string Env { get; set; }

        public static string TableStorageConnectionString { get; set; }

        private static IServiceProvider _provider;

        public static void Main(string[] args)
        {
            var settings = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddJsonFile("appsettings.personal.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            Env = (args.Length > 0) ? args[0] : settings["AppSynEnvironment"] ?? "Development";

            TableStorageConnectionString = settings["AppSynDataConnection"];

            var config = new JobHostConfiguration(TableStorageConnectionString);

            //if (config.IsDevelopment)
            if (String.Compare("Development", Env, StringComparison.OrdinalIgnoreCase) == 0)
            {
                config.UseDevelopmentSettings();
            }

            var services = new ServiceCollection();
            services.AddTagStorage(TableStorageConnectionString);
            services.AddTransient<IndexTagsCommand>();
            services.AddTransient<RecalculateDownloadCountsCommand>();

            _provider = services.BuildServiceProvider();

            var host = new JobHost(config);
            host.RunAndBlock();
        }

        public static async Task Index([QueueTrigger(StorageName.IndexQueue)] IndexChannelMessage message, TextWriter log)
        {
            using (var scope = _provider.GetRequiredService<IServiceScopeFactory>().CreateScope())
            {
                try
                {
                    var recalc = scope.ServiceProvider.GetService<RecalculateDownloadCountsCommand>();
                    await recalc.ExecuteAsync();

                    var index = scope.ServiceProvider.GetService<IndexTagsCommand>();
                    await index.ExecuteAsync();
                }
                catch (IndexChannelJobException e)
                {
                    await log.WriteLineAsync($"Failed to store message for {message.Channel} message: {e.Message}");
                }
            }
        }
    }
}