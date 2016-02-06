using System;
using AppSyndication.WebJobs.Data.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class TagEntity : TableEntity
    {
        public TagEntity() { }

        public TagEntity(string channel, string alias, string tagId, string version, string revision, string media, DateTime? stored = null)
        {
            if (String.IsNullOrEmpty(version))
            {
                version = "0";
            }

            if (String.IsNullOrEmpty(revision))
            {
                revision = "0";
            }

            this.PartitionKey = CalculatePartitionKey(channel);
            this.RowKey = CalculateRowKey(false, alias, media, version, revision);

            this.Channel = channel;
            this.Alias = alias;
            this.TagId = tagId;
            this.Version = version;
            this.Revision = revision;
            this.Media = media;
            this.Stored = stored ?? DateTime.UtcNow;
        }

        public string Uid => CalculateUid(this.PartitionKey, this.RowKey);

        public bool Primary => (this.RowKey == this.Alias);

        public string Channel { get; set; }

        public string Alias { get; set; }

        public string JsonBlobName => this.CalculateBlobUri("json");

        public string XmlBlobName => this.CalculateBlobUri("xml");

        public string Description { get; set; }

        public int DownloadCount { get; set; }

        public string[] Keywords { get; set; }

        public string LogoUri { get; set; }

        public string Media { get; set; }

        public string Name { get; set; }

        public string Revision { get; set; }

        public DateTime Stored { get; set; }

        public string TagId { get; set; }

        public string Version { get; set; }

        public TagEntity AsPrimary()
        {
            var tag = new TagEntity
            {
                PartitionKey = this.PartitionKey,
                RowKey = CalculateRowKey(true, this.Alias, this.Media, this.Version, this.Revision),
                Channel = this.Channel,
                Alias = this.Alias,
                Description = this.Description,
                Keywords = this.Keywords,
                LogoUri = this.LogoUri,
                Media = this.Media,
                Name = this.Name,
                Revision = this.Revision,
                Stored = this.Stored,
                TagId = this.TagId,
                Version = this.Version
            };

            return tag;
        }

        internal static string CalculatePartitionKey(string channel)
        {
            return channel;
        }

        internal static string CalculateRowKey(bool primaryTag, string alias, string media, string version, string revision)
        {
            var hash = HashMedia("|", media);

            return primaryTag ? $"{alias}{hash}" : $"{alias}{hash}|v{version}-r{revision}";
        }

        public static string CalculateUid(string partitionKey, string rowKey)
        {
            return String.Concat(partitionKey, "@", rowKey);
        }

        private string CalculateBlobUri(string extension)
        {
            var hashedMedia = HashMedia("-", this.Media);

            return $"{this.Channel}/{this.Alias}/v{this.Version}-r{this.Revision}{hashedMedia}.{extension}.swidtag".ToLowerInvariant();
        }

        private static string HashMedia(string prefix, string media)
        {
            if (String.IsNullOrEmpty(media))
            {
                return String.Empty;
            }

            return prefix + AzureUris.FriendlyHash(media);
        }
    }
}
