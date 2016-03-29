using System;
using FireGiant.AppSyndication.Processing.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing.Models
{
    public class TagEntity : TableEntity
    {
        public TagEntity() { }

        public TagEntity(string sourceAzid, string tagId, string version, string alias, string title, string description, string[] keywords, string fingerprint, DateTime updated)
        {
            this.SourceAzid = sourceAzid;
            this.TagAzid = AzureUris.AzureSafeId(tagId);

            this.PartitionKey = AzureUris.CalculateKey(this.SourceAzid, this.TagAzid);
            this.RowKey = version;

            this.Alias = alias;
            this.Description = description;
            this.Fingerprint = fingerprint;
            this.Keywords = keywords;
            this.TagId = tagId;
            this.Title = title;
            this.Updated = updated;
            this.Version = version;
        }

        public string Uid { get { return this.PartitionKey; } }

        public bool Primary { get { return String.IsNullOrEmpty(this.RowKey); } }

        public string Alias { get; set; }

        public string BlobJsonUri { get; set; }

        public string BlobXmlUri { get; set; }

        public string Description { get; set; }

        public int DownloadCount { get; set; }

        public string Fingerprint { get; set; }

        public string HistoryUri { get; set; }

        public string[] Keywords { get; set; }

        public string LogoUri { get; set; }

        public string SourceAzid { get; set; }

        public string TagAzid { get; set; }

        public string TagId { get; set; }

        public string Title { get; set; }

        public DateTime Updated { get; set; }

        public string Version { get; set; }

        public TagEntity AsPrimary()
        {
            var tag = new TagEntity();

            tag.PartitionKey = this.PartitionKey;
            tag.RowKey = String.Empty;
            tag.Alias = this.Alias;
            tag.BlobJsonUri = this.BlobJsonUri;
            tag.BlobXmlUri = this.BlobXmlUri;
            tag.Description = this.Description;
            tag.Fingerprint = this.Fingerprint;
            tag.HistoryUri = this.HistoryUri;
            tag.Keywords = this.Keywords;
            tag.LogoUri = this.LogoUri;
            tag.SourceAzid = this.SourceAzid;
            tag.TagAzid = this.TagAzid;
            tag.TagId = this.TagId;
            tag.Title = this.Title;
            tag.Updated = this.Updated;
            tag.Version = this.Version;

            return tag;
        }
    }
}
