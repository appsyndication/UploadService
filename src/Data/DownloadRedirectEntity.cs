using System;
using AppSyndication.WebJobs.Data.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class DownloadRedirectEntity : TableEntity
    {
        private string _redirectUri;

        public DownloadRedirectEntity() { }

        public DownloadRedirectEntity(string sourceAzid, string tagAzid, string tagUid, string tagVersion, string uri, string type, string media, string language)
        {
            this.PartitionKey = AzureUris.CalculateKey(sourceAzid, tagAzid, tagVersion, uri, type, media, language);
            this.RowKey = String.Empty;

            this.SourceAzid = sourceAzid;
            this.TagAzid = tagAzid;
            this.TagUid = tagUid;
            this.TagVersion = tagVersion;
            this.Uri = uri;
            this.Type = type;
            this.Media = media;
            this.Language = language;
        }

        public string Id { get { return this.PartitionKey; } }

        public string SourceAzid { get; set; }

        public string TagAzid { get; set; }

        public string TagUid { get; set; }

        public string TagVersion { get; set; }

        public string Language { get; set; }

        public string Media { get; set; }

        public string Type { get; set; }

        public string Uri { get; set; }

        public int DownloadCount { get; set; }

        public DateTime? DownloadCountLastUpdated { get; set; }

        public string RedirectUri => _redirectUri ?? (_redirectUri = "http://www.appsyndication.com/api/v1/downloads/" + this.PartitionKey);
    }
}
