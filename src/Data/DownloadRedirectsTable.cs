using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class DownloadRedirectsTable : TableBase
    {
        public DownloadRedirectsTable(Connection connection, bool ensureExists)
            : base("redirects", connection, ensureExists)
        {
        }

        public virtual IEnumerable<DownloadRedirectEntity> GetAllRedirects()
        {
            var query = new TableQuery<DownloadRedirectEntity>();

            var redirects = this.Table.ExecuteQuery(query);

            return redirects;
        }

        public virtual DownloadRedirectEntity GetRedirect(string redirectKey)
        {
            var op = TableOperation.Retrieve<DownloadRedirectEntity>(redirectKey, String.Empty);

            var result = this.Table.Execute(op);

            return (DownloadRedirectEntity)result.Result;
        }
    }
}
