using System;
using System.IO;
using System.Threading.Tasks;
using AppSyndication.WebJobs.Data;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace AppSyndication.WebJobs.IndexChannelJob
{
    public class Program
    {
        //public static Connection Connection { get; set; }
        public static string _connectionString;

        public static void Main(string[] args)
        {
            //if (Connection == null)
            //{
                _connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Storage);
            //}

            var config = new JobHostConfiguration()
            {
                DashboardConnectionString = _connectionString,
                StorageConnectionString = _connectionString,
            };

            if (config.IsDevelopment)
            {
                config.UseDevelopmentSettings();
            }

            var host = new JobHost(config);
            host.RunAndBlock();
        }

        public static async Task Index([QueueTrigger(StorageName.IndexQueue)] IndexChannelMessage message, TextWriter log)
        {
            var connection = new Connection(_connectionString);

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
