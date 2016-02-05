using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AppSyndication.WebJobs.Data;
using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Lucene.Net.Store.Azure;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AppSyndication.WebJobs.IndexChannelJob
{
    public class IndexTagsCommand
    {
        public IndexTagsCommand(Connection connection)
        {
            this.Connection = connection;
        }

        public bool DidWork { get; private set; }

        private Connection Connection { get; }

        public async Task<bool> ExecuteAsync()
        {
            this.DidWork = false;

            Console.WriteLine("Updating search service.");

            var txTable = this.Connection.TransactionTable();

            var systemInfo = txTable.GetSystemInfo();

            if (!systemInfo.LastIndexed.HasValue || systemInfo.LastIndexed < systemInfo.LastUpdatedStorage || systemInfo.LastIndexed < systemInfo.LastRecalculatedDownloadCount)
            {
                var tags = this.Connection
                    .TagTable()
                    .GetAllTags()
                    .ToList();

                var redirects = this.Connection
                    .RedirectTable()
                    .GetAllRedirects()
                    .ToList();

                Console.WriteLine("Updating search index to complete.");

                this.UpdateSearchIndex(tags, redirects);

                var change = txTable.Batch();

                systemInfo.LastIndexed = DateTime.UtcNow;
                change.CreateOrMerge(systemInfo);

                await change.WhenAll();

                this.DidWork = true;

                Console.WriteLine("Search service updated.");
            }

            return this.DidWork;
        }

        private static readonly string[] _fields = new[] { "alias", "name", "description", "keywords" };

        private void UpdateSearchIndex(IEnumerable<TagEntity> tags, IEnumerable<RedirectEntity> redirects)
        {
            var storage = this.Connection.ConnectToIndexStorage();

#if false
            var search = "wix";

            using (var azureDirectory = new AzureDirectory(storage, StorageName.SearchIndexBlobContainer))
            using (var indexReader = IndexReader.Open(azureDirectory, true))
            using (var searcher = new IndexSearcher(indexReader))
            {
                var query = new MultiFieldQueryParser(Lucene.Net.Util.Version.LUCENE_30, _fields, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30)).Parse(search);

                var results = searcher.Search(query, 50);

                foreach (var scoreDoc in results.ScoreDocs)
                {
                    Console.WriteLine("Tag #{0}", scoreDoc.Doc);

                    var d = searcher.Doc(scoreDoc.Doc);
                }
            }
#endif

            var workingFolder = Path.Combine(Path.GetTempPath(), typeof(IndexTagsCommand).ToString());

            using (var workingDirectory = FSDirectory.Open(workingFolder))
            using (var azureDirectory = new AzureDirectory(storage, StorageName.SearchIndexBlobContainer, workingDirectory))
            using (var indexWriter = CreateIndexWriter(azureDirectory))
            {
                foreach (var tag in tags)
                {
                    if (tag.Primary)
                    {
                        var document = CreateDocumentForPrimaryTag(tag);

                        indexWriter.AddDocument(document);
                    }
                    else
                    {
                        var document = CreateDocumentForHistory(tag);

                        indexWriter.AddDocument(document);
                    }
                }


                // Add fake data to bulk up the index.
#if FAKE_DATA
                    var s = "The Adobe Flash Player is freeware software for viewing multimedia, executing Rich Internet Applications, and streaming video and audio, content created on the Adobe Flash platform." +
                    "Chrome is a fast, simple, and secure web browser, built for the modern web." +
                    "Git (for Windows) Git is a powerful distributed Source Code Management tool. If you just want to use Git to do your version control in Windows, you will need to download Git for Windows, run the installer, and you are ready to start.\r\n\r\nNote: Git for Windows is a project run by volunteers, so if you want it to improve, volunteer!" +
                    "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum." +
                    "It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout. The point of using Lorem Ipsum is that it has a more-or-less normal distribution of letters, as opposed to using 'Content here, content here', making it look like readable English. Many desktop publishing packages and web page editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will uncover many web sites still in their infancy. Various versions have evolved over the years, sometimes by accident, sometimes on purpose (injected humour and the like)." +
                    "Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature, discovered the undoubtable source. Lorem Ipsum comes from sections de Finibus Bonorum et Malorum (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum Lorem ipsum dolor sit amet comes from a line in section" +
                    "The standard chunk of Lorem Ipsum used since the is reproduced below for those interested. Sections and from de Finibus Bonorum et Malorum by Cicero are also reproduced in their exact original form, accompanied by English versions from the 1914 translation by H. Rackham.";

                    var words = s.Split(' ');

                    var rnd = new Random();

                    for (int i = 1; i < 3001; ++i)
                    {
                        var name = Generate(rnd, words, 2, 5);

                        var alias = AzureUris.AzureSafeId(name);

                        var tagAzid = alias.ToLowerInvariant();

                        var description = Generate(rnd, words, 60, 120);

                        var keywords = Generate(rnd, words, 1, 6);

                        string version = String.Format("{0}.{1}.{2}", 10000 % i, 4000 % i, 3005 % i);

                        var downloads = rnd.Next(0, 2000000);

                        var imageUrl = String.Format("http://www.example.com/imgs/{0}.jpg", words[rnd.Next(words.Length)]);

                        var updated = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");

                        //updated = DateTime.UtcNow.ToString("o");

                        var url = (i % 2) == 1 ? "https://appsyndication.blob.core.windows.net/tags/sources/https-github-com-appsyndication-test-tree-master/http-wixtoolset-org-releases-wix4/v4.0.2220.0.json.swidtag" :
                            "https://appsyndication.blob.core.windows.net/tags/sources/https-github-com-appsyndication-test-tree-master/https-github-com-robmen-gitsetup/v1.9.4.40929.json.swidtag";

                        var document = new Document();
                        document.Add(new Field("id", tagAzid, Field.Store.YES, Field.Index.NOT_ANALYZED));
                        document.Add(new Field("alias", alias, Field.Store.YES, Field.Index.NOT_ANALYZED));
                        document.Add(new Field("name", name, Field.Store.YES, Field.Index.ANALYZED));
                        document.Add(new Field("description", description, Field.Store.YES, Field.Index.ANALYZED));
                        document.Add(new Field("keywords", keywords, Field.Store.YES, Field.Index.ANALYZED));
                        document.Add(new Field("tagSource", String.Empty, Field.Store.YES, Field.Index.NO));
                        document.Add(new Field("version", version, Field.Store.YES, Field.Index.NO));
                        document.Add(new Field("imageUrl", imageUrl, Field.Store.YES, Field.Index.NO));
                        document.Add(new Field("downloads", downloads.ToString(), Field.Store.YES, Field.Index.NO));
                        document.Add(new Field("tagUrl", url, Field.Store.YES, Field.Index.NO));
                        document.Add(new Field("updated", updated, Field.Store.YES, Field.Index.NO));

                        indexWriter.AddDocument(document);
                    }

                    Console.WriteLine("Total docs is {0}", indexWriter.NumDocs());
#endif

                foreach (var redirect in redirects)
                {
                    var document = CreateDocumentForRedirect(redirect);

                    indexWriter.AddDocument(document);
                }

                indexWriter.Optimize(true);
            }
        }

        private static Document CreateDocumentForPrimaryTag(TagEntity tag)
        {
            var keywords = String.Join(",", tag.Keywords ?? new string[0]);

            var document = new Document();
            document.Add(new Field("_type", "tag", Field.Store.NO, Field.Index.NOT_ANALYZED));
            document.Add(new Field("tag_uid", tag.Uid, Field.Store.YES, Field.Index.NOT_ANALYZED));
            document.Add(new Field("tag_channel", String.Empty, Field.Store.YES, Field.Index.NOT_ANALYZED));
            document.Add(new Field("tag_alias", tag.Alias ?? String.Empty, Field.Store.YES, Field.Index.NOT_ANALYZED));
            document.Add(new Field("tag_title", tag.Name, Field.Store.YES, Field.Index.ANALYZED));
            document.Add(new Field("tag_description", tag.Description, Field.Store.YES, Field.Index.ANALYZED));
            document.Add(new Field("tag_keywords", keywords, Field.Store.YES, Field.Index.ANALYZED));
            document.Add(new Field("tag_version", tag.Version, Field.Store.YES, Field.Index.NO));
            document.Add(new Field("tag_updated", tag.Stored.ToString("u"), Field.Store.YES, Field.Index.NO));
            document.Add(new Field("tag_logoUri", tag.LogoUri ?? String.Empty, Field.Store.YES, Field.Index.NO));
            document.Add(new Field("tag_blobJsonUri", tag.JsonBlobName, Field.Store.YES, Field.Index.NO));
            document.Add(new Field("tag_blobXmlUri", tag.XmlBlobName, Field.Store.YES, Field.Index.NO));
            document.Add(new Field("tag_downloads", tag.DownloadCount.ToString(), Field.Store.YES, Field.Index.NO));

            return document;
        }

        private static Document CreateDocumentForHistory(TagEntity tag)
        {
            var document = new Document();
            document.Add(new Field("_type", "history", Field.Store.NO, Field.Index.NOT_ANALYZED));
            document.Add(new Field("history_tagUid", tag.Uid, Field.Store.NO, Field.Index.NOT_ANALYZED));
            document.Add(new Field("history_tagTitle", tag.Name, Field.Store.YES, Field.Index.NO));
            document.Add(new Field("history_tagVersion", tag.Version, Field.Store.YES, Field.Index.NO));
            document.Add(new Field("history_tagUpdated", tag.Stored.ToString("u"), Field.Store.YES, Field.Index.NO));
            document.Add(new Field("history_downloads", tag.DownloadCount.ToString(), Field.Store.YES, Field.Index.NO));

            return document;
        }

        private static Document CreateDocumentForRedirect(RedirectEntity redirect)
        {
            var document = new Document();
            document.Add(new Field("_type", "redirect", Field.Store.NO, Field.Index.NOT_ANALYZED));
            document.Add(new Field("redirect_key", redirect.Id, Field.Store.YES, Field.Index.NOT_ANALYZED));
            document.Add(new Field("redirect_tagPk", redirect.TagPartitionKey, Field.Store.YES, Field.Index.NOT_ANALYZED));
            document.Add(new Field("redirect_tagRk", redirect.TagRowKey, Field.Store.YES, Field.Index.NOT_ANALYZED));
            document.Add(new Field("redirect_media", redirect.Media ?? String.Empty, Field.Store.YES, Field.Index.NO));
            document.Add(new Field("redirect_type", redirect.MediaType ?? String.Empty, Field.Store.YES, Field.Index.NO));
            document.Add(new Field("redirect_uri", redirect.Uri, Field.Store.YES, Field.Index.NO));

            return document;
        }

        private string Generate(Random random, string[] words, int min, int max)
        {
            var count = random.Next(min, max);

            var a = new string[count];

            for (int i = 0; i < count; ++i)
            {
                a[i] = words[random.Next(words.Length)];
            }

            return String.Join(" ", a, 0, count);
        }

        private static IndexWriter CreateIndexWriter(AzureDirectory azureDirectory)
        {
            IndexWriter indexWriter = null;

            while (indexWriter == null)
            {
                try
                {
                    indexWriter = new IndexWriter(azureDirectory, new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_30), /*!IndexReader.IndexExists(azureDirectory)*/true, new IndexWriter.MaxFieldLength(IndexWriter.DEFAULT_MAX_FIELD_LENGTH));
                }
                catch (LockObtainFailedException)
                {
                    Console.WriteLine("Lock is taken, waiting for timeout...");
                    Thread.Sleep(1000);
                }
            };

            Console.WriteLine("IndexWriter lock obtained, this process has exclusive write access to index");
            //indexWriter.SetRAMBufferSizeMB(10.0);
            //indexWriter.SetUseCompoundFile(false);
            //indexWriter.SetMaxMergeDocs(10000);
            //indexWriter.SetMergeFactor(100);

            return indexWriter;
        }


        private static CloudBlobContainer GetRootContainer(CloudBlobClient client)
        {
            var container = client.GetContainerReference("tags");

            var permissions = new BlobContainerPermissions() { PublicAccess = BlobContainerPublicAccessType.Blob };

            container.CreateIfNotExists();

            container.SetPermissions(permissions);

            return container;
        }

        private static CloudBlobDirectory GetTagSourceDirectory(CloudBlobContainer container, string sourceAzid)
        {
            var sources = container.GetDirectoryReference("sources");

            return sources.GetDirectoryReference(sourceAzid);
        }
    }
}
