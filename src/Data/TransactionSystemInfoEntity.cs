using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class TransactionSystemInfoEntity : TableEntity
    {
        public static readonly string PartitionKeyValue = String.Empty;

        public static readonly string RowKeyValue = String.Empty;

        public TransactionSystemInfoEntity()
        {
            this.PartitionKey = PartitionKeyValue;
            this.RowKey = RowKeyValue;
        }

        public DateTime? LastIngested { get; set; }

        public DateTime? LastUpdatedStorage { get; set; }

        public DateTime? LastRecalculatedDownloadCount { get; set; }

        public DateTime? LastIndexed { get; set; }
    }
}
