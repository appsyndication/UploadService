using System;
using System.Collections.Generic;
using AppSyndication.WebJobs.Data.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class TagsTable : TableBase
    {
        public TagsTable(Connection connection, bool ensureExists)
            : base("tags", connection, ensureExists)
        {
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
            return this.GetTag(sourceAzid, tagAzid, String.Empty);
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
