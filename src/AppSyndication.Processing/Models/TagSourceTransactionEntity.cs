using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing.Models
{
    public class TagSourceTransactionEntity : TableEntity
    {
        public TagSourceTransactionEntity() { }

        public TagSourceTransactionEntity(string transactionId, string sourceAzid)
        {
            this.PartitionKey = String.Empty;
            this.RowKey = "tsx|" + transactionId;

            this.TagSourceAzid = sourceAzid;
        }

        public string TransactionId { get { return this.RowKey; } }

        public string TagSourceAzid { get; set; }

        public DateTime? Ingested { get; set; }

        public DateTime? Stored { get; set; }

        public DateTime? Indexed { get; set; }
    }
}
