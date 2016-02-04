using System;
using System.Collections.Generic;
using FireGiant.AppSyndication.Processing.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing
{
    public class TagTransactionTable
    {
        public TagTransactionTable(Connection connection, bool ensureExists = true)
        {
            var storage = connection.ConnectToTagStorage();

            var tables = storage.CreateCloudTableClient();

            this.Table = tables.GetTableReference("tagstx");

            if (ensureExists)
            {
                this.Table.CreateIfNotExists();
            }
        }

        public TagTransactionTable(CloudTableClient tables, bool ensureExists = true)
        {
            this.Table = tables.GetTableReference("tagstx");

            if (ensureExists)
            {
                this.Table.CreateIfNotExists();
            }
        }

        public CloudTable Table { get; private set; }

        public TransactionInfoEntity GetTransactionInfo()
        {
            TransactionInfoEntity txInfo = new TransactionInfoEntity();

            var op = TableOperation.Retrieve<TransactionInfoEntity>(String.Empty, String.Empty);

            var result = this.Table.Execute(op);

            if (result.HttpStatusCode == 200)
            {
                txInfo = result.Result as TransactionInfoEntity;
            }

            return txInfo;
        }

        public IEnumerable<TagSourceTransactionEntity> GetTagSourceTransactions(string sourceAzid)
        {
            var rowFilter = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, "tsx{"),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, "tsx}")
                );

            var paritionFilter = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, String.Empty),
                TableOperators.And,
                rowFilter);

            var filter = TableQuery.CombineFilters(
                paritionFilter,
                TableOperators.And,
                TableQuery.GenerateFilterCondition("TagSourceAzid", QueryComparisons.Equal, sourceAzid)
                );

            var query = new TableQuery<TagSourceTransactionEntity>().Where(filter);

            var queriedChannelTransactions = this.Table.ExecuteQuery(query);

            return queriedChannelTransactions;
        }

        public IEnumerable<TagTransactionEntity> GetTagsInTransactions(string transactionId)
        {
            var filter = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, transactionId),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("Operation", QueryComparisons.NotEqual, "Error")
                );

            var query = new TableQuery<TagTransactionEntity>().Where(filter);

            var tagTransactions = this.Table.ExecuteQuery(query);

            return tagTransactions;
        }
    }
}
