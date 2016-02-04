using System;
using System.Collections.Generic;
using FireGiant.AppSyndication.Processing.Azure;
using FireGiant.AppSyndication.Processing.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing
{
    public class TagsTable
    {
        public TagsTable(Connection connection, bool ensureExists = true)
        {
            var storage = connection.ConnectToTagStorage();

            var tables = storage.CreateCloudTableClient();

            this.Table = tables.GetTableReference("tags");

            if (ensureExists)
            {
                this.Table.CreateIfNotExists();
            }
        }

        public TagsTable(CloudTableClient tables, bool ensureExists = true)
        {
            this.Table = tables.GetTableReference("tags");

            if (ensureExists)
            {
                this.Table.CreateIfNotExists();
            }
        }

        public CloudTable Table { get; private set; }

        public TagSourceEntity GetTagSource(string sourceAzid)
        {
            var tagSource = new TagSourceEntity();

            var op = TableOperation.Retrieve<TagSourceEntity>(sourceAzid, String.Empty);

            var result = this.Table.Execute(op);

            if (result.HttpStatusCode == 200)
            {
                tagSource = result.Result as TagSourceEntity;
            }

            return tagSource;
        }

        public IEnumerable<TagEntity> GetAllPrimaryTags()
        {
            var query = new TableQuery<TagEntity>().Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, String.Empty));

            var tags = this.Table.ExecuteQuery(query);

            return tags;
        }

        public IEnumerable<TagEntity> GetAllTags()
        {
            var query = new TableQuery<TagEntity>();

            var tags = this.Table.ExecuteQuery(query);

            return tags;
        }

        internal IEnumerable<TagEntity> GetAllTagsForAzid(string azid)
        {
            var filter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, azid);

            var query = new TableQuery<TagEntity>().Where(filter);

            var tags = this.Table.ExecuteQuery(query);

            return tags;
        }

        public TagEntity GetPrimaryTag(string sourceAzid, string tagAzid)
        {
            var azid = AzureUris.CalculateKey(sourceAzid, tagAzid);

            var op = TableOperation.Retrieve<TagEntity>(azid, String.Empty);

            var result = this.Table.Execute(op);

            return (TagEntity)result.Result;
        }

        public TagEntity GetTag(string sourceAzid, string tagAzid, string tagVersion)
        {
            var azid = AzureUris.CalculateKey(sourceAzid, tagAzid);

            var op = TableOperation.Retrieve<TagEntity>(azid, tagVersion);

            var result = this.Table.Execute(op);

            return (TagEntity)result.Result;
        }
    }
}
