using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Queue;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace AppSyndication.UploadService.Data
{
    public class Connection
    {
        private bool _redirectTableAlreadyExists;
        private bool _downloadTableAlreadyExists;
        private bool _tagTableAlreadyExists;
        private bool _transactionTableAlreadyExists;

        private bool _indexQueueAlreadyExists;
        private bool _tagContainerAlreadyExists;
        private bool _tagTransactionContainerAlreadyExists;
        private bool _tagTransactionQueueAlreadyExists;

        public Connection(string storageConnectionString)
        {
            this.StorageConnectionString = storageConnectionString;
        }

        public string StorageConnectionString { get; }

        private CloudStorageAccount TagStorage { get; set; }

        public CloudStorageAccount ConnectToIndexStorage()
        {
            return this.ConnectToTagStorage();
        }

        public async Task<CloudBlobContainer> TagContainerAsync(bool ensureExists = true)
        {
            var container = this.AccessBlobs().GetContainerReference(StorageName.TagBlobContainer);

            if (ensureExists && !_tagContainerAlreadyExists)
            {
                await container.CreateIfNotExistsAsync(BlobContainerPublicAccessType.Blob, new BlobRequestOptions() { DisableContentMD5Validation = true, }, new OperationContext());
                _tagContainerAlreadyExists = true;
            }

            return container;
        }

        public async Task<CloudBlobContainer> TagTransactionContainerAsync(bool ensureExists = true)
        {
            var container = this.AccessBlobs().GetContainerReference(StorageName.TagTransactionBlobContainer);

            if (ensureExists && !_tagTransactionContainerAlreadyExists)
            {
                await container.CreateIfNotExistsAsync();
                _tagTransactionContainerAlreadyExists = true;
            }

            return container;
        }

        public async Task<CloudBlockBlob> TagTransactionUploadBlobAsync(string channel, string transactionId)
        {
            var blobName = $"{channel}/{transactionId}";

            var container = await this.TagTransactionContainerAsync();

            return container.GetBlockBlobReference(blobName);
        }

        public async Task QueueTagTransactionMessageAsync(StoreTagMessage content)
        {
            await this.QueueMessageAsync(StorageName.TagTransactionQueue, content, _tagTransactionQueueAlreadyExists);

            _tagTransactionQueueAlreadyExists = true;
        }

        public async Task QueueIndexMessageAsync(IndexChannelMessage content)
        {
            await this.QueueMessageAsync(StorageName.IndexQueue, content, _indexQueueAlreadyExists);

            _indexQueueAlreadyExists = true;
        }

        public virtual RedirectTable RedirectTable(bool ensureExists = true)
        {
            return new RedirectTable(this, ensureExists, ref _redirectTableAlreadyExists);
        }

        public virtual DownloadTable DownloadTable(bool ensureExists = true)
        {
            return new DownloadTable(this, ensureExists, ref _downloadTableAlreadyExists);
        }

        public virtual TagTable TagTable(bool ensureExists = true)
        {
            return new TagTable(this, ensureExists, ref _tagTableAlreadyExists);
        }

        public virtual TransactionTable TransactionTable(bool ensureExists = true)
        {
            return new TransactionTable(this, ensureExists, ref _transactionTableAlreadyExists);
        }

        internal CloudTableClient AccessTables()
        {
            return this.ConnectToTagStorage().CreateCloudTableClient();
        }

        private CloudStorageAccount ConnectToTagStorage()
        {
            return this.TagStorage ?? (this.TagStorage = CloudStorageAccount.Parse(this.StorageConnectionString));
        }

        private CloudBlobClient AccessBlobs()
        {
            return this.ConnectToTagStorage().CreateCloudBlobClient();
        }

        private async Task QueueMessageAsync(string queueName, object content, bool alreadyExists)
        {
            var json = JsonConvert.SerializeObject(content);

            var message = new CloudQueueMessage(json);

            var queues = this.ConnectToTagStorage().CreateCloudQueueClient();

            var queue = queues.GetQueueReference(queueName);

            if (!alreadyExists)
            {
                await queue.CreateIfNotExistsAsync();
            }

            await queue.AddMessageAsync(message);
        }
    }
}
