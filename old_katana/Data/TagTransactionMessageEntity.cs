using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.UploadService.Data
{
    public class TagTransactionMessageEntity : TableEntity
    {
        private const int MaxMessageLength = 20 * 1024 * 1024;

        private static int Uniquifier = 0;

        public TagTransactionMessageEntity() { }

        public TagTransactionMessageEntity(TagTransactionEntity tagTransaction, string message)
        {
            this.PartitionKey = TagTransactionMessageEntity.CalculatePartitionKey(tagTransaction.Channel, tagTransaction.Id);
            this.RowKey = TagTransactionMessageEntity.CalculateRowKey();

            this.Channel = tagTransaction.Channel;
            this.Id = tagTransaction.Id;

            this.Operation = tagTransaction.Operation;
            this.Message = (message.Length > MaxMessageLength) ? message.Substring(0, MaxMessageLength) : message;
        }

        public string Channel { get; set; }

        public string Id { get; set; }

        public string Operation { get; set; }

        public string Message { get; set; }

        internal static string CalculatePartitionKey(string channel, string transactionId)
        {
            return TagTransactionEntity.CalculatePartitionKey(channel, transactionId);
        }

        internal static string CalculateRowKey()
        {
            var now = DateTime.UtcNow.ToString("yyyy-MMdd-HHmm-ss");

            var uniquifier = Uniquifier++;

            return $"msg|{now}-{uniquifier % 10000}";
        }
    }
}
