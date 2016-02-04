using System;
using FireGiant.AppSyndication.Processing.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing.Models
{
    public enum TagTransactionOperation
    {
        Error,
        Create,
        Update,
        Delete,
    }

    public class TagTransactionEntity : TableEntity
    {
        public TagTransactionEntity() { }

        public TagTransactionEntity(TagTransactionOperation operation, string transactionId, string fingerprint, string location, string blobUri, string alias, string tagId, string version, string title, string error = null)
        {
            this.PartitionKey = transactionId;
            this.RowKey = "tag|" + AzureUris.AzureSafeId(location);

            this.Operation = operation.ToString();

            this.Fingerprint = fingerprint;
            this.Location = location;
            this.Errors = error;
            this.StagedBlobUri = blobUri;

            this.Alias = alias;
            this.TagId = tagId;
            this.Version = version;
            this.Title = title;
            this.Updated = DateTime.UtcNow;
        }

        public string Id { get { return this.RowKey; } }

        public string Operation { get; set; }

        public string Location { get; set; }

        public string Alias { get; set; }

        public string TagId { get; set; }

        public string Version { get; set; }

        public string Fingerprint { get; set; }

        public string Title { get; set; }

        public DateTime Updated { get; set; }

        public string Errors { get; set; }

        public string StagedBlobUri { get; set; }

        public static TagTransactionEntity CreateDeleteTransaction(string transactionId, TagEntity tag)
        {
            var txTag = new TagTransactionEntity();

            txTag.PartitionKey = transactionId;
            txTag.RowKey = tag.TagAzid;

            txTag.Operation = TagTransactionOperation.Delete.ToString();

            txTag.Fingerprint = tag.Fingerprint;

            txTag.Title = tag.Title;

            txTag.Alias = tag.Alias;

            txTag.TagId = tag.TagId;

            txTag.Version = tag.Version;

            txTag.Updated = tag.Updated;

            return txTag;
        }

        public static TagTransactionEntity CreateErrorTransaction(string transactionId, string fingerprint, string location, string blobUri, string format, params string[] args)
        {
            var error = String.Format(format, args);

            return new TagTransactionEntity(TagTransactionOperation.Error, transactionId, fingerprint, location, blobUri, null, null, null, error);
        }

        public void AddError(string format, params string[] args)
        {
            var error = String.Format(format, args);

            this.Operation = TagTransactionOperation.Error.ToString();

            if (String.IsNullOrEmpty(this.Errors))
            {
                this.Errors = error;
            }
            else
            {
                this.Errors += "\r\n" + error;
            }
        }
    }
}
