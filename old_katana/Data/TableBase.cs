using System.Threading.Tasks;
using AppSyndication.UploadService.Data.Azure;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.UploadService.Data
{
    public abstract class TableBase
    {
        protected TableBase(string tableName, Connection connection, bool ensureExists, ref bool alreadyExists)
        {
            var tables = connection.AccessTables();

            this.Table = tables.GetTableReference(tableName);

            if (ensureExists && !alreadyExists)
            {
                this.Table.CreateIfNotExists();
                alreadyExists = true;
            }

            this.Connection = connection;
        }

        protected Connection Connection { get; }

        protected CloudTable Table { get; }

        public AzureBatch Batch()
        {
            return new AzureBatch(this.Table);
        }

        public async Task Create(ITableEntity entity)
        {
            var op = TableOperation.Insert(entity);

            await this.Table.ExecuteAsync(op);
        }

        public async Task CreateOrMergeAsync(ITableEntity entity)
        {
            var op = TableOperation.InsertOrMerge(entity);

            await this.Table.ExecuteAsync(op);
        }

        public async Task Update(ITableEntity entity)
        {
            var op = TableOperation.Merge(entity);

            await this.Table.ExecuteAsync(op);
        }

        public async Task Upsert(ITableEntity entity)
        {
            var op = TableOperation.InsertOrReplace(entity);

            await this.Table.ExecuteAsync(op);
        }

        public async Task Delete(ITableEntity entity)
        {
            var op = TableOperation.Delete(entity);

            await this.Table.ExecuteAsync(op);
        }

        public Task<TableResult> ExecuteAsync(TableOperation operation)
        {
            return this.Table.ExecuteAsync(operation);
        }
    }
}
