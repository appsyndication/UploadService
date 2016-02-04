using System;
using FireGiant.AppSyndication.Data;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing.Models
{
    public class TagSourceEntity : TableEntity
    {
        public TagSourceEntity() { }

        public TagSourceEntity(TagSource source)
        {
            this.PartitionKey = Azure.AzureUris.AzureSafeId(source.Uri);

            this.RowKey = String.Empty;

            this.Type = source.Type;

            this.Uri = source.Uri;
        }

        public string TagSourceAzid { get { return this.PartitionKey; } }

        // TODO: auth eventually.

        public DateTime? LastUpdated { get; set; }

        public TagSourceType Type { get; set; }

        public string Uri { get; set; }
    }
}
