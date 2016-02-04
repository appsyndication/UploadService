using AppSyndication.WebJobs.Data.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data
{
    public abstract class TableBase
    {
        protected TableBase(string tableName, Connection connection, bool ensureExists)
        {
            var tables = connection.AccessTables();

            this.Table = tables.GetTableReference(tableName);

            if (ensureExists)
            {
                this.Table.CreateIfNotExists();
            }

            this.Connection = connection;
        }

        protected Connection Connection { get; }

        protected CloudTable Table { get; }

        public AzureBatch Change()
        {
            return new AzureBatch(this.Table);
        }
    }
}
