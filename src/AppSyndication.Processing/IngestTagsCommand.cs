using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using FearTheCowboy.Iso19770;
using FireGiant.AppSyndication.Data;
using FireGiant.AppSyndication.Processing.Azure;
using FireGiant.AppSyndication.Processing.Ingest;
using FireGiant.AppSyndication.Processing.Models;
using Microsoft.WindowsAzure.Storage.Table;

namespace FireGiant.AppSyndication.Processing
{
    public class IngestTagsCommand
    {
        public Connection Connection { private get; set; }

        public TagSource Source { private get; set; }

        public bool DidWork { get; private set; }

        public async Task<bool> ExecuteAsync()
        {
            Foo();

            var storage = this.Connection.ConnectToTagStorage();

            var tables = storage.CreateCloudTableClient();

            await this.EnsureChannel();

            var txTable = new TagTransactionTable(tables);

            var tagsTable = new TagsTable(tables);

            var txInfo = txTable.GetTransactionInfo();

            var sourceAzid = this.Source.AzureId();

            TagSourceTransactionEntity sourceTx;
            if (!TryGetTagSourceTransaction(txTable.Table, sourceAzid, txInfo.LastIngested, out sourceTx))
            {
                Trace.TraceInformation("Already ingested tag source but not yet stored, skip processing this source until later, source: {0}", this.Source.Uri);

                return this.DidWork = false;
            }

            var existingTags = tagsTable.GetAllPrimaryTags().Where(t => t.SourceAzid == sourceAzid);

            IEnumerable<TagTransactionEntity> tagTxs;

            Console.WriteLine("Calculating tag transactions.");
            if (this.Source.Type == TagSourceType.Github)
            {
                var command = new CalculateTagTransactionsFromGithubRepositoryCommand();
                command.Connection = this.Connection;
                command.ExistingTags = existingTags;
                command.TagSource = (GithubTagSource)this.Source;
                command.TransactionId = sourceTx.TransactionId;

                var tagTxTasks = command.ExecuteAsync();

                tagTxs = await tagTxTasks;
            }
            else
            {
                throw new InvalidCastException(String.Format("Unsupported tag source type: {0}", this.Source.Type));
            }

            Console.WriteLine("Finding duplicate tags.");
            FindDuplicateTags(tagTxs);

            //Console.WriteLine("Writing ingested tags to transaction table.");
            //{
            //    var command = new CreateTagTransactionsCommand();
            //    command.TransactionTable = txTable.Table;
            //    command.TransactionId = sourceTx.TransactionId;
            //    command.ExistingTags = existingTags;
            //    command.IngestedTags = ingestedTags;
            //    var tagTxs = command.ExecuteAsync();

            //    await tagTxs;

            //    this.DidWork = command.FoundWork;
            //}

            var txBatch = new AzureBatch(txTable.Table);

            if (tagTxs.Any())
            {
                this.DidWork = true;

                foreach (var tagTx in tagTxs)
                {
                    txBatch.Create(tagTx);
                }
            }

            await txBatch.WhenAll();

            sourceTx.Ingested = DateTime.UtcNow;
            txBatch.CreateOrMerge(sourceTx);

            // Ensuring the last ingested timestamp is at least one millisecond after the source transaction allows
            // us to later look for transactions equal to or later than this transaction and not pick up the source
            // transaction we *just* created.
            //
            txInfo.LastIngested = DateTime.UtcNow.AddMilliseconds(1);
            txBatch.CreateOrMerge(txInfo);

            await txBatch.WhenAll();

            return this.DidWork;
        }

        private void Foo()
        {
            var c = new System.Net.WebClient();
            var d = c.DownloadData("http://localhost:5002/");
            var s = System.Text.Encoding.UTF8.GetString(d);

            var t = SoftwareIdentity.LoadHtml(s);

            var l = t.Links;
        }

        private async Task EnsureChannel()
        {
            var storage = this.Connection.ConnectToTagStorage();

            var tables = storage.CreateCloudTableClient();

            var table = tables.GetTableReference("channels");

            table.CreateIfNotExists();

            var tagSource = new TagSourceEntity(this.Source);

            var batch = new AzureBatch(table);

            batch.CreateOrMerge(tagSource);

            await batch.WhenAll();
        }

        private static bool TryGetTagSourceTransaction(CloudTable txTable, string sourceAzid, DateTime? lastIngested, out TagSourceTransactionEntity sourceTx)
        {
            var command = new GetIngestTagSourceTransactionCommand();
            command.TransactionTable = txTable;
            command.SourceAzid = sourceAzid;
            command.LastIngested = lastIngested;
            command.Execute();

            sourceTx = command.SourceTransaction;

            return !command.SourceTransactionOutstanding;
        }

        private static void FindDuplicateTags(IEnumerable<TagTransactionEntity> tagTransactions)
        {
            var collisions = tagTransactions
                .ToLookup(t => t.TagId ?? String.Empty)
                .Where(g => !String.IsNullOrEmpty(g.Key) && g.Count() > 1);

            foreach (var collision in collisions)
            {
                TagTransactionEntity first = null;

                foreach (var x in collision)
                {
                    if (first == null)
                    {
                        first = x;
                    }
                    else
                    {
                        first.AddError("Tag id: {0} is duplicated with file: {1}", first.TagId, x.Location);
                        x.AddError("Tag id: {0} is duplicated with file: {1}", x.TagId, first.Location);
                    }
                }
            }
        }
    }
}
