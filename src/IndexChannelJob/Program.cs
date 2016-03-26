using System;
using System.IO;
using System.Threading.Tasks;
using AppSyndication.BackendModel.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.WebJobs;

namespace AppSyndication.WebJobs.IndexChannelJob
{
    public static class Program
    {
        public static string Env { get; set; }

        public static string TableStorageConnectionString { get; set; }

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

            var host = new JobHost(config);
            host.RunAndBlock();
        }

        public static async Task Index([QueueTrigger(StorageName.IndexQueue)] IndexChannelMessage message, TextWriter log)
        {
            var connection = new Connection(TableStorageConnectionString /*_environment.TableStorageConnectionString*/);

            try
            {
                var recalc = new RecalculateDownloadCountsCommand(connection);
                await recalc.ExecuteAsync();

                var index = new IndexTagsCommand(connection);
                await index.ExecuteAsync();
            }
            catch (IndexChannelJobException e)
            {
                await log.WriteLineAsync($"Failed to store message for {message.Channel} message: {e.Message}");
            }
        }
    }
}
