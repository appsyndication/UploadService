using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace AppSyndication.WebJobs.Data
{
    public class Connection
    {
        public const string TagTransactionBlobContainer = "tagtxs";

        public const string TagTransactionQueue = "tag-queue";

        public Connection(string storageConnectionString)
        {
            this.StorageConnectionString = storageConnectionString;
        }

        public string StorageConnectionString { get; }

        private CloudStorageAccount TagStorage { get; set; }

        public CloudStorageAccount ConnectToTagStorage()
        {
            return this.TagStorage ?? (this.TagStorage = CloudStorageAccount.Parse(this.StorageConnectionString));
        }

        public CloudBlobClient AccessBlobs()
        {
            return this.ConnectToTagStorage().CreateCloudBlobClient();
        }

        public CloudTableClient AccessTables()
        {
            return this.ConnectToTagStorage().CreateCloudTableClient();
        }

        public async Task QueueMessageAsync(string queueName, object content)
        {
            var json = JsonConvert.SerializeObject(content);

            var message = new CloudQueueMessage(json);

            var queues = this.ConnectToTagStorage().CreateCloudQueueClient();

            var queue = queues.GetQueueReference(queueName);

            await queue.CreateIfNotExistsAsync();

            await queue.AddMessageAsync(message);
        }

        public virtual DownloadRedirectsTable DownloadRedirectsTable(bool ensureExists = true)
        {
            return new DownloadRedirectsTable(this, ensureExists);
        }

        public virtual DownloadsTable DownloadsTable(bool ensureExists = true)
        {
            return new DownloadsTable(this, ensureExists);
        }

        public virtual TagsTable TagsTable(bool ensureExists = true)
        {
            return new TagsTable(this, ensureExists);
        }

        public virtual TransactionTable TransactionTable(bool ensureExists = true)
        {
            return new TransactionTable(this, ensureExists);
        }
    }
}
