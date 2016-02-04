using System;
using System.Diagnostics;
using FireGiant.AppSyndication.Processing.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing.Ingest
{
    public class GetIngestTagSourceTransactionCommand
    {
        public string SourceAzid { private get; set; }

        public DateTimeOffset? LastIngested { private get; set; }

        public CloudTable TransactionTable { private get; set; }

        public bool SourceTransactionOutstanding { get; private set; }

        public TagSourceTransactionEntity SourceTransaction { get; private set; }

        public void Execute()
        {
            var rowFilter = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, "tsx{"),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, "tsx}")
                );

            var filter = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, String.Empty),
                TableOperators.And,
                TableQuery.CombineFilters(
                    rowFilter,
                    //TableOperators.And,
                    //TableQuery.CombineFilters(
                    //    TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThanOrEqual, this.LastIngested),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("TagSourceId", QueryComparisons.Equal, this.SourceAzid)
                        //)
                    )
                );

            var query = new TableQuery<TagSourceTransactionEntity>().Where(filter);

            var queriedTagSourceTransactions = this.TransactionTable.ExecuteQuery(query);

            foreach (var sourceTx in queriedTagSourceTransactions)
            {
                Debug.Assert(sourceTx.TagSourceAzid == this.SourceAzid);

                if (sourceTx.Ingested == null)
                {
                    // TODO: this commented out code doesn't behave well... so reusing the transaction instead.
                    //Console.WriteLine("Already an outstanding request for this source to be ingested.");

                    //this.SourceTransaction = sourceTx;
                    //this.SourceTransactionOutstanding = true;

                    //return;
                    this.SourceTransaction = new TagSourceTransactionEntity(sourceTx.TransactionId, this.SourceAzid);
                    this.SourceTransactionOutstanding = false;
                    return;
                }
            }

            var transactionId = Guid.NewGuid().ToString("N");

            var tx = new TagSourceTransactionEntity(transactionId, this.SourceAzid);

            var op = TableOperation.Insert(tx);
            var result = this.TransactionTable.Execute(op);

            this.SourceTransaction = tx;
            this.SourceTransactionOutstanding = false;
        }
    }
}
