using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AppSyndication.WebJobs.Data
{
    public class StartTagTransaction
    {
        private static int Uniquifier = 0;

        private StartTagTransaction(Connection connection, TagTransactionEntity entity, CloudBlockBlob blob)
        {
            this.Connection = connection;
            this.Entity = entity;
            this.Blob = blob;
        }

        private Connection Connection { get; }

        private TagTransactionEntity Entity { get; }

        private CloudBlockBlob Blob { get; }

        public static async Task<StartTagTransaction> CreateAsync(Connection connection, string channel, string username)
        {
            if (String.IsNullOrEmpty(username)) throw new ArgumentNullException(nameof(username));

            channel = String.IsNullOrEmpty(channel) ? "@" : channel;

            var now = DateTime.UtcNow.ToString("yyyy-MMdd-HHmm-ss");

            var uniquifier = Uniquifier++;

            var transactionId = $"{username}|{now}-{uniquifier % 10000}";

            var stagedBlob = $"{channel}/{transactionId}";

            var blobs = connection.AccessBlobs();

            var container = blobs.GetContainerReference(Connection.TagTransactionBlobContainer);

            await container.CreateIfNotExistsAsync();

            var blob = container.GetBlockBlobReference(stagedBlob);

            var entity = new TagTransactionEntity(TagTransactionOperation.Create, channel, transactionId, stagedBlob);

            var change = connection.TransactionTable().Change();

            change.Create(entity);

            await change.WhenAll();

            return new StartTagTransaction(connection, entity, blob);
        }

        public async Task CompleteAsync()
        {
            this.Entity.Stored = DateTime.UtcNow;

            var change = this.Connection.TransactionTable().Change();

            change.Update(this.Entity);

            await change.WhenAll();

            var message = new StagedTagMessage(this.Entity.Channel, this.Entity.Id);

            await this.Connection.QueueMessageAsync(Connection.TagTransactionQueue, message);
        }

        public void SetFilename(string filename)
        {
            this.Entity.OriginalFilename = filename;
        }

        public Stream GetWriteStream()
        {
            return this.Blob.OpenWrite();
        }

        public async Task WriteToStream(Stream stream)
        {
            await this.Blob.UploadFromStreamAsync(stream);
        }
    }
}
