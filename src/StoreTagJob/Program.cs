using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using AppSyndication.WebJobs.Data;
using FearTheCowboy.Iso19770;
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

        public static async Task StoreTag([QueueTrigger(StorageName.TagTransactionQueue)] StoreTagMessage message, string channel, string transactionId, int dequeueCount, [Blob("tagtx/{channel}/{transactionId}", FileAccess.Read)] Stream input, TextWriter log)
        {
            //var connectionString = AmbientConnectionStringProvider.Instance.GetConnectionString(ConnectionStringNames.Storage);
            var connection = new Connection(_connectionString);

            var tagTxTable = connection.TransactionTable();

            var tagTx = await tagTxTable.GetTagTransactionAsync(channel, transactionId);

            if (tagTx == null)
            {
                throw new InvalidDataException($"Could not find tag transaction for channel: {channel} id: {transactionId}");
            }

            try
            {
                var tag = await ReadTag(tagTx, input);

                var command = new UpdateStorageCommand(connection, tagTx, tag);
                await command.ExecuteAsync();

                await connection.QueueIndexMessageAsync(new IndexChannelMessage(channel));
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

        private static async Task<SoftwareIdentity> ReadTag(TagTransactionEntity tagTx, Stream stream)
        {
            SoftwareIdentity tag;

            var bytes = new byte[1024 * 1024]; // all tags must be less than a MB.

            var read = await stream.ReadAsync(bytes, 0, bytes.Length);

            if (read == bytes.Length)
            {
                throw new StoreTagJobException("Tag must be less that 1 MB.");
            }

            var text = Encoding.UTF8.GetString(bytes, 0, read);

            if (!TryLoadJsonTag(text, out tag))
            {
                if (!TryLoadXmlTag(text, out tag))
                {
                    throw new StoreTagJobException($"Cannot parse tag: {text}");
                }
            }

            return tag;
        }

        private static bool TryLoadJsonTag(string text, out SoftwareIdentity tag)
        {
            try
            {
                tag = SoftwareIdentity.LoadJson(text);
            }
            catch (Exception)
            {
                tag = null;
            }

            return tag != null;
        }

        private static bool TryLoadXmlTag(string text, out SoftwareIdentity tag)
        {
            try
            {
                tag = SoftwareIdentity.LoadXml(text);
            }
            catch (Exception)
            {
                tag = null;
            }

            return tag != null;
        }
    }
}
