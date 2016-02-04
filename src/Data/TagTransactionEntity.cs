using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public enum TagTransactionOperation
    {
        Unknown,
        Error,
        Create,
        Update,
        Delete,
    }

    public class TagTransactionEntity : TableEntity
    {
        public TagTransactionEntity() { }

        public TagTransactionEntity(TagTransactionOperation operation, string channel, string transactionId, string blobUri)
        {
            this.PartitionKey = TagTransactionEntity.CalculatePartitionKey(channel, transactionId);
            this.RowKey = TagTransactionEntity.CalculateRowKey();

            this.Channel = channel;
            this.Id = transactionId;

            this.Operation = operation.ToString();
            this.StagedBlobUri = blobUri;
        }

        public string Channel { get; set; }

        public string Id { get; set; }

        public string Operation { get; set; }

        public string OriginalFilename { get; set; }

        public string StagedBlobUri { get; set; }

        public DateTime? Stored { get; set; }

        public DateTime? Updated { get; set; }

        public TagTransactionOperation OperationValue
        {
            get
            {
                TagTransactionOperation operation;
                return Enum.TryParse<TagTransactionOperation>(this.Operation, out operation) ? operation : TagTransactionOperation.Unknown;
            }
        }

        public bool TryUpdateOperation(TagTransactionOperation operation)
        {
            if (this.OperationValue != operation)
            {
                this.Operation = operation.ToString();
                return true;
            }

            return false;
        }

        //public static TagTransactionEntity CreateDeleteTransaction(string transactionId, TagEntity tag)
        //{
        //    var txTag = new TagTransactionEntity();

        //    txTag.PartitionKey = transactionId;
        //    txTag.RowKey = tag.TagAzid;

        //    txTag.Operation = TagTransactionOperation.Delete.ToString();

        //    txTag.Fingerprint = tag.Fingerprint;

        //    txTag.Name = tag.Name;

        //    txTag.Alias = tag.Alias;

        //    txTag.TagId = tag.TagId;

        //    txTag.Version = tag.Version;

        //    txTag.Updated = tag.Updated;

        //    return txTag;
        //}

        internal static string CalculatePartitionKey(string channel, string transactionId)
        {
            return channel + "|" + transactionId;
        }

        internal static string CalculateRowKey()
        {
            return String.Empty;
        }

        /// <summary>
        /// Creates a tag transaction entity that can be used to update the entity in table storage
        /// as an error, without updating any other fields of the table entity. This is useful when
        /// there were changes made to the entity before an error was found and you do not want to
        /// push all the changes (just mark the transaction entity as an error).
        /// </summary>
        /// <returns></returns>
        public TagTransactionEntity AsError()
        {
            return new TagTransactionEntity
            {
                PartitionKey = this.PartitionKey,
                RowKey = this.RowKey,
                Operation = TagTransactionOperation.Error.ToString(),
            };
        }
    }
}
