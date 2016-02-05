using System;
using System.IO;
using System.Threading.Tasks;
using AppSyndication.WebJobs.Data;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace AppSyndication.WebJobs.StoreTagJob
{
    public static class Program
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

        public static async Task StoreTag([QueueTrigger(StorageName.TagTransactionQueue)] StoreTagMessage message, string channel, string transactionId, int dequeueCount, TextWriter log)
        {
            //var connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Storage);
            var connection = new Connection(_connectionString);

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
