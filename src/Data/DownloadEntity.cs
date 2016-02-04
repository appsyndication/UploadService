using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class DownloadEntity : TableEntity
    {
        public DownloadEntity() { }

        public DownloadEntity(DateTime when, string downloadKey, string ip)
        {
            this.PartitionKey = String.Concat(when.ToString("yyyy-MM-ddTHH"), "|", downloadKey);
            this.RowKey = Guid.NewGuid().ToString("N");

            this.DownloadKey = downloadKey;

            this.IP = ip;
        }

        public string DownloadKey { get; }

        public string IP { get; set; }
    }
}
