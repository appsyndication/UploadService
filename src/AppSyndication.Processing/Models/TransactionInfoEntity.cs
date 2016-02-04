using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing.Models
{
    public class TransactionInfoEntity : TableEntity
    {
        public TransactionInfoEntity()
        {
            this.PartitionKey = String.Empty;
            this.RowKey = String.Empty;
        }

        public DateTime? LastIngested { get; set; }

        public DateTime? LastUpdatedStorage { get; set; }

        public DateTime? LastRecalculatedDownloadCount { get; set; }

        public DateTime? LastIndexed { get; set; }
    }
}
