using System;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public class RedirectTable : TableBase
    {
        public RedirectTable(Connection connection, bool ensureExists, ref bool alreadyExists)
            : base(StorageName.RedirectTable, connection, ensureExists, ref alreadyExists)
        {
        }

        public virtual IEnumerable<RedirectEntity> GetAllRedirects()
        {
            var query = new TableQuery<RedirectEntity>();

            var redirects = this.Table.ExecuteQuery(query);

            return redirects;
        }

        public virtual RedirectEntity GetRedirect(string redirectKey)
        {
            var op = TableOperation.Retrieve<RedirectEntity>(redirectKey, String.Empty);

            var result = this.Table.Execute(op);

            return (RedirectEntity)result.Result;
        }
    }
}
