using System;
using System.Collections.Generic;
using FireGiant.AppSyndication.Processing.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing
{
    public class DownloadRedirectsTable
    {
        public DownloadRedirectsTable(Connection connection, bool ensureExists = true)
        {
            var storage = connection.ConnectToTagStorage();

            var tables = storage.CreateCloudTableClient();

            this.Table = tables.GetTableReference("redirects");

            if (ensureExists)
            {
                this.Table.CreateIfNotExists();
            }
        }

        public DownloadRedirectsTable(CloudTableClient tables, bool ensureExists = true)
        {
            this.Table = tables.GetTableReference("redirects");

            if (ensureExists)
            {
                this.Table.CreateIfNotExists();
            }
        }

        public CloudTable Table { get; private set; }

        public IEnumerable<DownloadRedirectEntity> GetAllRedirects()
        {
            var query = new TableQuery<DownloadRedirectEntity>();

            var redirects = this.Table.ExecuteQuery(query);

            return redirects;
        }

        public DownloadRedirectEntity GetRedirect(string redirectKey)
        {
            var op = TableOperation.Retrieve<DownloadRedirectEntity>(redirectKey, String.Empty);

            var result = this.Table.Execute(op);

            return (DownloadRedirectEntity)result.Result;
        }
    }
}
