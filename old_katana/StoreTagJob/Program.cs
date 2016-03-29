using System;
using System.IO;
using System.Threading.Tasks;
using AppSyndication.UploadService.Data;
using Microsoft.Azure.WebJobs;

namespace AppSyndication.WebJobs.StoreTagJob
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

        public static async Task StoreTag([QueueTrigger(StorageName.TagTransactionQueue)] StoreTagMessage message, string channel, string transactionId, int dequeueCount, TextWriter log)
        {
            var connection = new Connection(_environment.TableStorageConnectionString);

            var tagTxTable = connection.TransactionTable();

            var tagTx = await tagTxTable.GetTagTransactionAsync(channel, transactionId);

            if (tagTx == null)
            {
                await log.WriteLineAsync($"Could not find transaction id: {transactionId} in channel: {channel}");
                return;
            }

            try
            {
                var update = new UpdateStorageCommand(connection, tagTx);
                await update.ExecuteAsync();

                if (update.DidWork)
                {
                    await connection.QueueIndexMessageAsync(new IndexChannelMessage(channel));
                }
            }
            catch (StoreTagJobException e)
            {
                try
                {
                    await tagTxTable.AddTagTransactionErrorMessageAsync(tagTx, e.Message);
                }
                catch (Exception exception)
                {
                    await log.WriteLineAsync($"Failed to store message for {channel}/{transactionId}. Original message: {e.Message}. Exception: {exception.ToString()}");
                }
            }
        }
    }
}
