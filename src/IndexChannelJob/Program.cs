using System.IO;
using System.Threading.Tasks;
using AppSyndication.WebJobs.Data;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace AppSyndication.WebJobs.IndexChannelJob
{
    public class Program
    {
        public static Connection Connection { get; set; }

        public static void Main(string[] args)
        {
            if (Connection == null)
            {
                var connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Storage);
                Connection = new Connection(connectionString);
            }

            var config = new JobHostConfiguration()
            {
                DashboardConnectionString = Connection.StorageConnectionString,
                StorageConnectionString = Connection.StorageConnectionString,
            };

            if (config.IsDevelopment)
            {
                config.UseDevelopmentSettings();
            }

            var host = new JobHost(config);
            host.RunAndBlock();
        }

        public static async Task Index([QueueTrigger("tag-index-queue")] string channel, TextWriter log)
        {
            log.WriteLine("Indexing started");

            await RecalculateDownloadCounts(channel, log);

            var index = new IndexTagsCommand(Connection);
            await index.ExecuteAsync();
        }

        public static async Task<bool> RecalculateDownloadCounts(string channel, TextWriter log)
        {
            log.WriteLine("Recalculating download counts");

            var command = new RecalculateDownloadCountsCommand(Connection);
            await command.ExecuteAsync();

            //if (command.DidWork)
            //{
            //    await this.QueueTagSource(ProcessActionType.Index, null);
            //}

            return true;

        }
    }
}
