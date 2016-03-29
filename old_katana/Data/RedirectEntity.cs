using System;
using AppSyndication.UploadService.Data.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.UploadService.Data
{
    public class RedirectEntity : TableEntity
    {
        private string _redirectUri;

        public RedirectEntity() { }

        public RedirectEntity(string tagPartitionKey, string tagRowKey, string uri, string mediaType, string media, string language)
        {
            this.PartitionKey = CalculatePartitionKey(tagPartitionKey, tagRowKey, uri, mediaType, media, language);
            this.RowKey = CalculateRowKey();

            this.TagPartitionKey = tagPartitionKey;
            this.TagRowKey = tagRowKey;

            this.Uri = uri;
            this.MediaType = mediaType;
            this.Media = media;
            this.Language = language;
        }

        public string Id => this.PartitionKey;

        public string TagPartitionKey { get; set; }

        public string TagRowKey { get; set; }

        public string Language { get; set; }

        public string Media { get; set; }

        public string MediaType { get; set; }

        public string Uri { get; set; }

        public int DownloadCount { get; set; }

        public DateTime? DownloadCountLastUpdated { get; set; }

        public string RedirectUri => _redirectUri ?? (_redirectUri = "http://www.appsyndication.com/api/v1/downloads/" + this.PartitionKey);

        internal static string CalculatePartitionKey(string tagPartitionKey, string tagRowKey, string uri, string type, string media, string language)
        {
            return AzureUris.CalculateKey(tagPartitionKey, tagRowKey, uri, type, media, language);
        }

        internal static string CalculateRowKey()
        {
            return String.Empty;
        }
    }
}
