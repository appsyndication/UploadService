using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class TagTable : TableBase
    {
        public TagTable(Connection connection, bool ensureExists, ref bool alreadyExists)
            : base(StorageName.TagTable, connection, ensureExists, ref alreadyExists)
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
    }
}
