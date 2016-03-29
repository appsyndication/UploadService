using System.IO;
using System.Threading.Tasks;
using AppSyndication.UploadService.Data;
using Microsoft.Azure.WebJobs;

namespace AppSyndication.WebJobs.IndexChannelJob
{
    public static class Program
    {
        public static UploadServiceEnvironmentConfiguration _environment;

        public static void Main(string[] args)
        {
            _environment = new UploadServiceEnvironmentConfiguration();

            var config = new JobHostConfiguration()
            {
                DashboardConnectionString = _environment.TableStorageConnectionString,
                StorageConnectionString = _environment.TableStorageConnectionString,
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
            var connection = new Connection(_environment.TableStorageConnectionString);

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
