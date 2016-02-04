using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class TransactionTable : TableBase
    {
        public TransactionTable(Connection connection, bool ensureExists)
            : base("tagstx", connection, ensureExists)
        {
        }

        public virtual TransactionSystemInfoEntity GetSystemInfo()
        {
            var op = TableOperation.Retrieve<TransactionSystemInfoEntity>(TransactionSystemInfoEntity.PartitionKeyValue, TransactionSystemInfoEntity.RowKeyValue);

            var result = this.Table.Execute(op);

            return (TransactionSystemInfoEntity)result.Result;
        }

        public virtual DownloadRedirectEntity GetRedirect(string redirectKey)
        {
            var op = TableOperation.Retrieve<DownloadRedirectEntity>(redirectKey, String.Empty);

            var result = this.Table.Execute(op);

            return (DownloadRedirectEntity)result.Result;
        }

        public async Task<TagTransactionEntity> GetTagTransactionAsync(string channel, string transactionId)
        {
            var partitionKey = TagTransactionEntity.CalculatePartitionKey(channel, transactionId);

            var rowKey = TagTransactionEntity.CalculateRowKey();

            var op = TableOperation.Retrieve<TagTransactionEntity>(partitionKey, rowKey);

            var result = await this.Table.ExecuteAsync(op);

            return (TagTransactionEntity)result.Result;
        }

        public async Task AddTagTransactionErrorMessageAsync(TagTransactionEntity entity, string message)
        {
            var change = this.Change();

            if (entity.TryUpdateOperation(TagTransactionOperation.Error))
            {
                var errorEntity = entity.AsError();

                change.Update(errorEntity);
            }

            var messageEntity = new TagTransactionMessageEntity(entity, message);

            change.Create(messageEntity);

            await change.WhenAll();
        }
    }
}
