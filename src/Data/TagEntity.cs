using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class TagEntity : TableEntity
    {
        public TagEntity() { }

        public TagEntity(string channel, string alias, string tagId, string version, string revision, string media, DateTime? stored = null)
        {
            if (String.IsNullOrEmpty(revision))
            {
                revision = "0";
            }

            this.PartitionKey = CalculatePartitionKey(channel, alias, media);
            this.RowKey = CalculateRowKey(false, version, revision);

            this.Channel = channel;
            this.Alias = alias;
            this.TagId = tagId;
            this.Version = version;
            this.Revision = revision;
            this.Stored = stored.HasValue ? stored.Value : DateTime.UtcNow;
        }

        public string Uid => this.PartitionKey;

        public bool Primary => String.IsNullOrEmpty(this.RowKey);

        public string Channel { get; set; }

        public string Alias { get; set; }

        public string BlobJsonUri { get; set; }

        public string BlobXmlUri { get; set; }

        public string Description { get; set; }

        public int DownloadCount { get; set; }

        public string[] Keywords { get; set; }

        public string LogoUri { get; set; }

        public string Revision { get; set; }

        public DateTime Stored { get; set; }

        public string TagId { get; set; }

        public string Name { get; set; }

        public string Version { get; set; }

        public TagEntity AsPrimary()
        {
            var tag = new TagEntity
            {
                PartitionKey = this.PartitionKey,
                RowKey = CalculateRowKey(true, this.Version, this.Revision),
                Alias = this.Alias,
                BlobJsonUri = this.BlobJsonUri,
                BlobXmlUri = this.BlobXmlUri,
                Description = this.Description,
                Keywords = this.Keywords,
                LogoUri = this.LogoUri,
                Name = this.Name,
                TagId = this.TagId,
                Stored = this.Stored,
                Version = this.Version
            };

            return tag;
        }

        internal static string CalculatePartitionKey(string channel, string alias, string media)
        {
            var hash = HashMedia(media);

            return $"{channel}|{alias}{hash}";
        }

        internal static string CalculateRowKey(bool primaryTag, string version, string revision)
        {
            return primaryTag ? String.Empty : $"v{version}-r{revision}";
        }

        private static string HashMedia(string media)
        {
            if (String.IsNullOrEmpty(media))
            {
                return String.Empty;
            }

            // TODO: implement hashing
            throw new NotImplementedException();
        }
    }
}
