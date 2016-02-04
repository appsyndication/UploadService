using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace AppSyndication.WebJobs.Data.Azure
{
    public class AzureBatch
    {
        public AzureBatch(CloudTable table)
        {
            this.Batch = new TableBatchOperation();
            this.Table = table;
            this.Tasks = new List<Task<IList<TableResult>>>();
        }

        public CloudTable Table { get; private set; }

        private TableBatchOperation Batch { get; set; }

        private List<Task<IList<TableResult>>> Tasks { get; set; }

        public void Create(ITableEntity entity)
        {
            this.Batch.Insert(entity);

            this.ExecuteIfNecessary();
        }

        public void CreateOrMerge(ITableEntity entity)
        {
            this.Batch.InsertOrMerge(entity);

            this.ExecuteIfNecessary();
        }

        public void Update(ITableEntity entity)
        {
            this.Batch.Merge(entity);

            this.ExecuteIfNecessary();
        }

        public void Upsert(ITableEntity entity)
        {
            this.Batch.InsertOrReplace(entity);

            this.ExecuteIfNecessary();
        }

        public void Delete(ITableEntity entity)
        {
            this.Batch.Delete(entity);

            this.ExecuteIfNecessary();
        }

        public async Task<IList<TableResult>[]> WhenAll()
        {
            this.ExecuteIfAnything();

            return await Task.WhenAll(this.Tasks).ConfigureAwait(false);
        }

        private void ExecuteIfAnything()
        {
            if (this.Batch.Count > 0)
            {
                var batch = this.Batch;

                this.Batch = new TableBatchOperation();

                var task = this.Table.ExecuteBatchAsync(batch);
                this.Tasks.Add(task);
            }
        }

        private void ExecuteIfNecessary()
        {
            if (this.Batch.Count > 100)
            {
                var batch = this.Batch;

                this.Batch = new TableBatchOperation();

                var task = this.Table.ExecuteBatchAsync(batch);
                this.Tasks.Add(task);
            }
        }
    }
}
