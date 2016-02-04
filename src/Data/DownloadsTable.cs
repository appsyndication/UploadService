using System;
using System.Collections.Generic;
using AppSyndication.WebJobs.Data.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class DownloadsTable : TableBase
    {
        public DownloadsTable(Connection connection, bool ensureExists)
            : base("downloads", connection, ensureExists)
        {
        }

        public virtual IEnumerable<DownloadRedirectEntity> GetAllRedirects()
        {
            var query = new TableQuery<DownloadRedirectEntity>();

            var redirects = this.Table.ExecuteQuery(query);

            return redirects;
        }

        public virtual IEnumerable<DownloadEntity> GetDownloadsSince(DateTime? start)
        {
            var startOffset = new DateTimeOffset(start ?? AzureDateTime.Min);

            var filter = TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, startOffset);

            var query = new TableQuery<DownloadEntity>().Where(filter);

            return this.Table.ExecuteQuery(query);
        }
    }
}
