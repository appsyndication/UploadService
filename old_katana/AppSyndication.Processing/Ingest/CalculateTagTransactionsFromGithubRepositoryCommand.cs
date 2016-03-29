using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FearTheCowboy.Iso19770;
using FireGiant.AppSyndication.Data;
using FireGiant.AppSyndication.Processing.Azure;
using FireGiant.AppSyndication.Processing.Models;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Octokit;

namespace FireGiant.AppSyndication.Processing.Ingest
{
    public class CalculateTagTransactionsFromGithubRepositoryCommand
    {
        public Connection Connection { private get; set; }

        public IEnumerable<TagEntity> ExistingTags { private get; set; }

        public GithubTagSource TagSource { private get; set; }

        public string TransactionId { private get; set; }

        public IEnumerable<TagTransactionEntity> TagTransactions { get; private set; }

        public IEnumerable<TagTransactionEntity> Execute()
        {
            var tagTxs = this.ExecuteAsync();

            return tagTxs.Result;
        }

        public async Task<IEnumerable<TagTransactionEntity>> ExecuteAsync()
        {
            var results = new List<TagTransactionEntity>();

            var existingTagsByAzid = this.ExistingTags.ToDictionary(t => t.TagAzid);

            var storage = this.Connection.ConnectToTagStorage();

            var blobs = storage.CreateCloudBlobClient();

            var sourceDirectory = GetTagSourceDirectory(blobs, this.TagSource.AzureId());

            var github = new GitHubClient(new ProductHeaderValue("AppSyndication-WebJob"));

            var queue = new Queue<GithubFolder>();

            queue.Enqueue(new GithubFolder() { Reference = this.TagSource.Branch, Path = "/" });

            while (queue.Count > 0)
            {
                var container = queue.Dequeue();

                var tree = await github.GitDatabase.Tree.Get(this.TagSource.Owner, this.TagSource.Repository, container.Reference);

                foreach (var item in tree.Tree)
                {
                    switch (item.Type)
                    {
                        case TreeType.Blob:
                            var blob = await github.GitDatabase.Blob.Get(this.TagSource.Owner, this.TagSource.Repository, item.Sha);
                            var result = await this.CreateTagTransactionFromGithubBlob(sourceDirectory, existingTagsByAzid, container.Path + item.Path, blob);

                            if (result != null)
                            {
                                results.Add(result);
                            }
                            break;

                        case TreeType.Tree:
                            var child = new GithubFolder() { Path = container.Path + item.Path + "/", Reference = item.Sha };
                            queue.Enqueue(child);

                            Console.WriteLine("Adding child: {0}", child.Path);
                            break;
                    }
                }
            }

            // Any existing tags left over must have been deleted at the source
            // so create delete transactions for them.
            //
            foreach (var obsoleteTag in existingTagsByAzid.Values)
            {
                var deleteTagTx = TagTransactionEntity.CreateDeleteTransaction(this.TransactionId, obsoleteTag);

                results.Add(deleteTagTx);
            }

            return this.TagTransactions = results;
        }

        private async Task<TagTransactionEntity> CreateTagTransactionFromGithubBlob(CloudBlobDirectory sourceDirectory, Dictionary<string, TagEntity> existingTagsByAzid, string location, Blob blob)
        {
            Console.WriteLine("Parsing: {0}", location);

            TagTransactionEntity result = null;

            byte[] bytes = null;

            var fingerprint = blob.Sha;

            var azureBlob = sourceDirectory.GetBlockBlobReference(AzureUris.AzureSafeId(location) + "/" + fingerprint);

            try
            {
                bytes = GithubBlobContentAsUtf8(blob);

                var json = Encoding.UTF8.GetString(bytes, 0, bytes.Length);

                var swidtag = SoftwareIdentity.LoadJson(json);

                if (swidtag == null)
                {
                    result = TagTransactionEntity.CreateErrorTransaction(this.TransactionId, fingerprint, location, azureBlob.Uri.AbsoluteUri, "Unknown error while parsing tag as json");
                }
                else
                {
                    var tagAzid = swidtag.AzureId();

                    var tagTitle = GetTagTitle(swidtag);

                    if (String.IsNullOrEmpty(tagTitle))
                    {
                        result = TagTransactionEntity.CreateErrorTransaction(this.TransactionId, fingerprint, location, azureBlob.Uri.AbsoluteUri, "Tag must provide 'title' metadata to be included in the catalog.");
                    }
                    else
                    {
                        TagEntity existingTag = null;

                        if (existingTagsByAzid.TryGetValue(tagAzid, out existingTag))
                        {
                            existingTagsByAzid.Remove(tagAzid);
                        }

                        if (existingTag == null ||
                            String.IsNullOrEmpty(existingTag.Fingerprint) ||
                            !existingTag.Fingerprint.Equals(fingerprint, StringComparison.Ordinal))
                        {
                            var op = (existingTag == null) ? TagTransactionOperation.Create : TagTransactionOperation.Update;

                            result = new TagTransactionEntity(op, this.TransactionId, fingerprint, location, azureBlob.Uri.AbsoluteUri, swidtag.Name, swidtag.TagId, swidtag.Version, tagTitle);
                        }
                    }
                }
            }
            catch (JsonReaderException e)
            {
                result = TagTransactionEntity.CreateErrorTransaction(this.TransactionId, fingerprint, location, azureBlob.Uri.AbsoluteUri, "Unexpected error while parsing tag as json, detail: {0}", e.Message);
            }

            if (result != null && bytes != null)
            {
                await azureBlob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);
            }

            return result;
        }

        private static byte[] GithubBlobContentAsUtf8(Blob blob)
        {
            return (blob.Encoding == EncodingType.Base64) ? Convert.FromBase64String(blob.Content) : Encoding.UTF8.GetBytes(blob.Content);
        }

        private static CloudBlobDirectory GetTagSourceDirectory(CloudBlobClient client, string sourceAzid)
        {
            var container = client.GetContainerReference("tagtxs");

            var sources = container.GetDirectoryReference("sources");

            container.CreateIfNotExists();

            return sources.GetDirectoryReference(sourceAzid);
        }

        private string GetTagTitle(SoftwareIdentity swidtag)
        {
            var meta = swidtag.Meta.FirstOrDefault();

            return meta?["title"];
        }

        private class GithubFolder
        {
            public string Reference { get; set; }

            public string Path { get; set; }
        }
    }
}
