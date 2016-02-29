using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.UploadService.Data
{
    public class TransactionTable : TableBase
    {
        public TransactionTable(Connection connection, bool ensureExists, ref bool alreadyExists)
            : base(StorageName.TransactionTable, connection, ensureExists, ref alreadyExists)
        {
        }

        public virtual TransactionSystemInfoEntity GetSystemInfo()
        {
            var op = TableOperation.Retrieve<TransactionSystemInfoEntity>(TransactionSystemInfoEntity.PartitionKeyValue, TransactionSystemInfoEntity.RowKeyValue);

            var result = this.Table.Execute(op);

            return (TransactionSystemInfoEntity)result.Result ?? new TransactionSystemInfoEntity();
        }

        public virtual RedirectEntity GetRedirect(string redirectKey)
        {
            var op = TableOperation.Retrieve<RedirectEntity>(redirectKey, String.Empty);

            var result = this.Table.Execute(op);

            return (RedirectEntity)result.Result;
        }

        public virtual async Task<TagTransactionEntity> GetTagTransactionAsync(string channel, string transactionId)
        {
            var partitionKey = TagTransactionEntity.CalculatePartitionKey(channel, transactionId);

            var rowKey = TagTransactionEntity.CalculateRowKey();

            var op = TableOperation.Retrieve<TagTransactionEntity>(partitionKey, rowKey);

            var result = await this.Table.ExecuteAsync(op);

            return (TagTransactionEntity)result.Result;
        }

        public virtual async Task AddTagTransactionErrorMessageAsync(TagTransactionEntity entity, string message)
        {
            var change = this.Batch();

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
