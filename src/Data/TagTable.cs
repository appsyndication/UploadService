using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class TagTable : TableBase
    {
        public TagTable(Connection connection, bool ensureExists, ref bool alreadyExists)
            : base(StorageName.TagTable, connection, ensureExists, ref alreadyExists)
        {
        }

        public virtual async Task<TagEntity> GetTagAsync(string partitionKey, string rowKey)
        {
            var op = TableOperation.Retrieve<TagEntity>(partitionKey, rowKey);

            var result = await this.Table.ExecuteAsync(op);

            return (TagEntity)result.Result;
        }

        public virtual async Task<TagEntity> GetPrimaryTagAsync(TagEntity tag)
        {
            var partitionKey = TagEntity.CalculatePartitionKey(tag.Channel);

            var rowKey = TagEntity.CalculateRowKey(true, tag.Alias, tag.Media, tag.Version, tag.Revision);

            var op = TableOperation.Retrieve<TagEntity>(partitionKey, rowKey);

            var result = await this.Table.ExecuteAsync(op);

            return (TagEntity)result.Result;
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
    }
}
