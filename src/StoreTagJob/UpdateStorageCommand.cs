using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using AppSyndication.BackendModel.Data;
using FearTheCowboy.Iso19770;
using Microsoft.WindowsAzure.Storage.Blob;

namespace AppSyndication.WebJobs.StoreTagJob
{
    public class UpdateStorageCommand
    {
        public UpdateStorageCommand(Connection connection, TagTransactionEntity tagTransaction)
        {
            this.Connection = connection;

            this.TagTransaction = tagTransaction;
        }

        public bool DidWork { get; private set; }

        private Connection Connection { get; }

        private TagTransactionEntity TagTransaction { get; }

        public async Task<bool> ExecuteAsync()
        {
            var txTable = this.Connection.TransactionTable();

            var txInfo = txTable.GetSystemInfo();

            var operation = this.TagTransaction.OperationValue;

            switch (operation)
            {
                case TagTransactionOperation.Create:
                    await this.CreateTag();
                    break;

                default:
                    throw new NotImplementedException();
            }

            this.TagTransaction.Stored = DateTime.UtcNow;
            await txTable.CreateOrMergeAsync(this.TagTransaction);

            txInfo.LastUpdatedStorage = DateTime.UtcNow.AddMilliseconds(1);
            await txTable.CreateOrMergeAsync(txInfo);

            return this.DidWork = true;
        }

        private async Task<TagEntity> CreateTag()
        {
            var swidtag = await ReadTag(this.Connection, this.TagTransaction);

            var tagEntity = CreateTagEntityFromSoftwareIdentity(this.TagTransaction.Channel, this.TagTransaction.AliasOverride, swidtag);

            var redirects = UpdateInstallationMediaLinksInSoftwareIdentityAndReturnWithRedirects(tagEntity.PartitionKey, tagEntity.RowKey, swidtag).ToList();

            await this.WriteRedirects(redirects);

            await this.WriteBlobs(tagEntity, swidtag);

            tagEntity.Stored = DateTime.UtcNow;

            await this.WriteTag(tagEntity);

            return tagEntity;
        }
        
        private static async Task<SoftwareIdentity> ReadTag(Connection connection, TagTransactionEntity tagTx)
        {
            SoftwareIdentity swidtag;

            var blob = await connection.TagTransactionUploadBlobAsync(tagTx.Channel, tagTx.Id);

            var text = await blob.DownloadTextAsync();

            if (!TryLoadJsonTag(text, out swidtag))
            {
                if (!TryLoadXmlTag(text, out swidtag))
                {
                    throw new StoreTagJobException($"Cannot parse tag: {text}");
                }
            }

            return swidtag;
        }

        private static bool TryLoadJsonTag(string text, out SoftwareIdentity tag)
        {
            try
            {
                tag = SoftwareIdentity.LoadJson(text);
            }
            catch (Exception)
            {
                tag = null;
            }

            return tag != null;
        }

        private static bool TryLoadXmlTag(string text, out SoftwareIdentity tag)
        {
            try
            {
                tag = SoftwareIdentity.LoadXml(text);
            }
            catch (Exception)
            {
                tag = null;
            }

            return tag != null;
        }

        private static TagEntity CreateTagEntityFromSoftwareIdentity(string channel, string alias, SoftwareIdentity swidtag)
        {
            var name = swidtag.Name;
            var version = swidtag.Version;
            var revision = swidtag.TagVersion;
            var media = swidtag.AppliesToMedia;

            string description = null;
            string keywordText = null;
            string logoUri = null;

            foreach (var meta in swidtag.Meta)
            {
                if (String.IsNullOrEmpty(alias))
                {
                    alias = meta["alias"];
                }

                if (String.IsNullOrEmpty(description))
                {
                    description = meta.Description;
                }

                if (String.IsNullOrEmpty(keywordText))
                {
                    keywordText = meta["keyword"];
                }
            }

            if (String.IsNullOrEmpty(name))
            {
                throw new StoreTagJobException("An name is required. Add an name attribute to SoftwareIdentity element.");
            }

            if (String.IsNullOrEmpty(alias))
            {
                throw new StoreTagJobException("An alias is required. Add an alias attribute to Meta element or specify an alias when uploading the tag.");
            }

            foreach (var link in swidtag.Links)
            {
                if (String.IsNullOrEmpty(logoUri) && link.Relationship == "logoUri")
                {
                    logoUri = link.HRef.AbsoluteUri;
                }
            }

            var entity = new TagEntity(channel, alias, swidtag.TagId, version, revision, media)
            {
                Description = description,
                LogoUri = logoUri,
                Keywords = keywordText?.Split(new[] { ' ', '\t', ',', ';' }, StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToArray(),
                Name = name,
            };

            return entity;
        }

        private static IEnumerable<RedirectEntity> UpdateInstallationMediaLinksInSoftwareIdentityAndReturnWithRedirects(string tagPartitionKey, string tagRowKey, SoftwareIdentity swidtag)
        {
            foreach (var installationLink in swidtag.Links.Where(l => l.Relationship == FearTheCowboy.Iso19770.Schema.Relationship.InstallationMedia))
            {
                var redirect = new RedirectEntity(tagPartitionKey, tagRowKey, installationLink.HRef.AbsoluteUri, installationLink.MediaType, installationLink.Media, null);

                swidtag.RemoveLink(installationLink.HRef);

                var redirectUri = new Uri(redirect.RedirectUri);

                swidtag.AddLink(redirectUri, FearTheCowboy.Iso19770.Schema.Relationship.InstallationMedia);

                yield return redirect;
            }
        }

        private async Task WriteRedirects(IEnumerable<RedirectEntity> redirects)
        {
            var table = this.Connection.RedirectTable();

            foreach (var redirect in redirects)
            {
                await table.Upsert(redirect);
            }
        }

        private async Task WriteBlobs(TagEntity tagEntity, SoftwareIdentity swidtag)
        {
            var tagContainer = await this.Connection.TagContainerAsync();

            var json = swidtag.SwidTagJson;

            var jsonBlob = tagContainer.GetBlockBlobReference(tagEntity.JsonBlobName);

            var xml = swidtag.SwidTagXml;

            var xmlBlob = tagContainer.GetBlockBlobReference(tagEntity.XmlBlobName);

            await Task.WhenAll(
                UploadTagToBlob(tagEntity, jsonBlob, json, FearTheCowboy.Iso19770.Schema.MediaType.SwidTagJsonLd),
                UploadTagToBlob(tagEntity, xmlBlob, xml, FearTheCowboy.Iso19770.Schema.MediaType.SwidTagXml)
                );
        }

        private async Task WriteTag(TagEntity tagEntity)
        {
            var primaryTagEntity = tagEntity.AsPrimary();

            var batch = this.Connection.TagTable().Batch();

            batch.Upsert(tagEntity);

            batch.Upsert(primaryTagEntity);

            await batch.WhenAll();
        }
        
        private static async Task UploadTagToBlob(TagEntity tag, ICloudBlob blob, string content, string contentType)
        {
            blob.Properties.ContentType = contentType;
            blob.Metadata.Add("id", tag.TagId);
            blob.Metadata.Add("uid", tag.Uid);
            blob.Metadata.Add("version", tag.Version);
            blob.Metadata.Add("revision", tag.Revision);

            // TODO: it would be nice if we could pre-gzip our tags in storage but that requires the client to accept
            //       gzip which not enough people seem to do.
            //blob.Properties.ContentEncoding = "gzip";
            //using (var stream = new MemoryStream(bytes.Length))
            //{
            //    using (var gzip = new GZipStream(stream, CompressionLevel.Optimal, true))
            //    {
            //        gzip.Write(bytes, 0, bytes.Length);
            //    }

            //    stream.Seek(0, SeekOrigin.Begin);
            //    blob.UploadFromStream(stream);
            //}

            var bytes = Encoding.UTF8.GetBytes(content);

            await blob.UploadFromByteArrayAsync(bytes, 0, bytes.Length);
        }

#if false

        private async Task ProcessTransactions(string sourceAzid, CloudBlobDirectory sourceDirectory, IEnumerable<TagTransactionEntity> tagTransactions, SoftwareIdentity indexJsonTag, SoftwareIdentity indexXmlTag)
        {
            var storage = this.Connection.ConnectToTagStorage();

            var tagsTable = new TagTable(this.Connection);

            //var tagsBatch = new AzureBatch(tagsTable.Table);

            var redirectsTable = new DownloadRedirectsTable(this.Connection);

            var batchTasks = new List<AzureBatch>();

            var redirectsTasks = new List<Task>();

            var addedTags = new List<AddedTagResult>();

            var updatedTags = new List<UpdatedTagResult>();

            var deleteTags = new List<DeleteTagResult>();

            foreach (var tagTx in tagTransactions)
            {
                var op = (TagTransactionOperation)Enum.Parse(typeof(TagTransactionOperation), tagTx.Operation);

                switch (op)
                {
                    case TagTransactionOperation.Create:
                        {
                            var add = await this.CreateTag(sourceAzid, sourceDirectory, tagTx);

                            addedTags.Add(add);

                            var batch = new AzureBatch(tagsTable.Table);
                            batch.Create(add.Tag);
                            batch.Upsert(add.PrimaryTag);
                            batchTasks.Add(batch);

                            var tasks = CreateRedirects(redirectsTable.Table, add.Redirects);
                            redirectsTasks.AddRange(tasks);
                        }
                        break;

                    case TagTransactionOperation.Update:
                        {
                            var update = await this.UpdateTag(sourceAzid, sourceDirectory, tagsTable, tagTx);

                            updatedTags.Add(update);

                            var batch = new AzureBatch(tagsTable.Table);
                            batch.Create(update.Tag);
                            batch.Upsert(update.PrimaryTag);
                            batchTasks.Add(batch);

                            var tasks = CreateRedirects(redirectsTable.Table, update.Redirects);
                            redirectsTasks.AddRange(tasks);
                        }
                        break;

                    case TagTransactionOperation.Delete:
                        {
                            var delete = DeleteTag(sourceAzid, tagsTable, tagTx);

                            deleteTags.Add(delete);

                            var batch = new AzureBatch(tagsTable.Table);
                            batch.Update(delete.Tag);
                            batchTasks.Add(batch);
                        }
                        break;
                }
            }

            foreach (var batch in batchTasks)
            {
                await batch.WhenAll();
            }

            await Task.WhenAll(redirectsTasks);

            if (this.UpdateIndexTags(indexJsonTag, indexXmlTag, addedTags, updatedTags, deleteTags))
            {
                await this.WriteIndexTags(indexJsonTag, indexXmlTag, sourceDirectory);
            }

            var deleteTasks = new List<Task>(deleteTags.Count + 1);

            foreach (var deleteResult in deleteTags)
            {
                var blobJson = new CloudBlockBlob(deleteResult.DeleteSourceUris.JsonUri, storage.Credentials);

                var blobXml = new CloudBlockBlob(deleteResult.DeleteSourceUris.XmlUri, storage.Credentials);

                deleteTasks.Add(blobJson.DeleteIfExistsAsync());

                deleteTasks.Add(blobXml.DeleteIfExistsAsync());
            }

            await Task.WhenAll(deleteTasks);
        }

        private async Task<SoftwareIdentity> GetIndexSoftwareIdentity(CloudBlobDirectory sourcesDirectory, string contentType)
        {
            var extension = (contentType == FearTheCowboy.Iso19770.Schema.MediaType.SwidTagJsonLd) ? "json" : "xml";

            var indexBlob = sourcesDirectory.GetBlockBlobReference(String.Format("index.{0}.SoftwareIdentity", extension));

            if (indexBlob.Exists())
            {
                var text = await indexBlob.DownloadTextAsync();

                return (contentType == FearTheCowboy.Iso19770.Schema.MediaType.SwidTagJsonLd) ? SoftwareIdentity.LoadJson(text) : SoftwareIdentity.LoadXml(text);
            }

            return new SoftwareIdentity() { Name = "AppSyndication Index", TagId = indexBlob.Uri.AbsoluteUri };
        }

        private CloudBlobDirectory GetTagSourceDirectory(string sourceAzid)
        {
            var storage = this.Connection.ConnectToTagStorage();

            var client = storage.CreateCloudBlobClient();

            EnableCors(client);

            var container = client.GetContainerReference("tags");

            var permissions = new BlobContainerPermissions() { PublicAccess = BlobContainerPublicAccessType.Blob };

            container.CreateIfNotExists();

            container.SetPermissions(permissions);

            var sources = container.GetDirectoryReference("sources");

            return sources.GetDirectoryReference(sourceAzid);
        }

        private static void EnableCors(CloudBlobClient client)
        {
            var serviceProperties = client.GetServiceProperties();

            serviceProperties.Cors = new CorsProperties();
            serviceProperties.Cors.CorsRules.Add(new CorsRule()
            {
                AllowedHeaders = new List<string>() { "*" },
                AllowedMethods = CorsHttpMethods.Get | CorsHttpMethods.Head,
                AllowedOrigins = new List<string>() { "*" },
                ExposedHeaders = new List<string>() { "*" },
                MaxAgeInSeconds = 1800 // 30 minutes
            });

            client.SetServiceProperties(serviceProperties);
        }

        private async Task<AddedTagResult> CreateTag(string sourceAzid, CloudBlobDirectory sourceDirectory, TagTransactionEntity tagTx)
        {
            var json = await this.GetBlobAsString(tagTx.StagedBlobUri);

            var softwareIdentity = SoftwareIdentity.LoadJson(json);

            var tagEntity = this.CreateTagEntityFromSoftwareIdentity(sourceAzid, tagTx, softwareIdentity);

            var redirects = UpdateInstallationMediaLinksInSoftwareIdentityAndReturnWithRedirects(sourceAzid, tagEntity.TagAzid, tagEntity.Uid, softwareIdentity);

            var tagUris = await WriteVersionedTag(sourceDirectory, tagEntity, softwareIdentity);

            tagEntity.JsonBlobName = tagUris.JsonUri.AbsoluteUri;

            tagEntity.XmlBlobName = tagUris.XmlUri.AbsoluteUri;

            var primaryTag = tagEntity.AsPrimary();

            return new AddedTagResult() { NewSourceUris = tagUris, Redirects = redirects, Tag = tagEntity, PrimaryTag = primaryTag };
        }

        private async Task<UpdatedTagResult> UpdateTag(string sourceAzid, CloudBlobDirectory sourceDirectory, TagTable tagsTable, TagTransactionEntity tagTx)
        {
            var oldTagEntity = tagsTable.GetPrimaryTag(sourceAzid, tagTx.Id);

            var json = await this.GetBlobAsString(tagTx.StagedBlobUri);

            var softwareIdentity = SoftwareIdentity.LoadJson(json);

            var tagEntity = this.CreateTagEntityFromSoftwareIdentity(sourceAzid, tagTx, softwareIdentity);

            var redirects = UpdateInstallationMediaLinksInSoftwareIdentityAndReturnWithRedirects(sourceAzid, tagEntity.TagAzid, tagEntity.Uid, softwareIdentity);

            var tagUris = await WriteVersionedTag(sourceDirectory, tagEntity, softwareIdentity);

            tagEntity.JsonBlobName = tagUris.JsonUri.AbsoluteUri;

            tagEntity.XmlBlobName = tagUris.XmlUri.AbsoluteUri;

            var primaryTag = tagEntity.AsPrimary();

            primaryTag.DownloadCount = oldTagEntity.DownloadCount;

            return new UpdatedTagResult()
            {
                NewSourceUris = tagUris,
                OldSourceUris = new TagBlobUris
                {
                    JsonUri = new Uri(oldTagEntity.JsonBlobName),
                    XmlUri = new Uri(oldTagEntity.XmlBlobName)
                },
                Redirects = redirects,
                Tag = tagEntity,
                PrimaryTag = primaryTag
            };
        }

        private static DeleteTagResult DeleteTag(string sourceAzid, TagTable tagsTable, TagTransactionEntity tagTx)
        {
            var tag = tagsTable.GetPrimaryTag(sourceAzid, tagTx.RowKey);

            tag.RowKey = "deleted";

            return new DeleteTagResult()
            {
                DeleteSourceUris = new TagBlobUris
                {
                    JsonUri = new Uri(tag.JsonBlobName),
                    XmlUri = new Uri(tag.XmlBlobName)
                },
                Tag = tag
            };
        }

        private bool UpdateIndexTags(SoftwareIdentity indexJsonTag, SoftwareIdentity indexXmlTag, List<AddedTagResult> addedTags, List<UpdatedTagResult> updatedTags, List<DeleteTagResult> deleteTags)
        {
            var any = false;

            foreach (var added in addedTags)
            {
                indexJsonTag.AddLink(added.NewSourceUris.JsonUri, "package");
                indexXmlTag.AddLink(added.NewSourceUris.XmlUri, "package");

                any = true;
            }

            foreach (var update in updatedTags)
            {
                indexJsonTag.RemoveLink(update.OldSourceUris.JsonUri);
                indexXmlTag.RemoveLink(update.OldSourceUris.XmlUri);

                indexJsonTag.AddLink(update.NewSourceUris.JsonUri, "package");
                indexXmlTag.AddLink(update.NewSourceUris.XmlUri, "package");

                any = true;
            }

            foreach (var uri in deleteTags)
            {
                indexJsonTag.RemoveLink(uri.DeleteSourceUris.JsonUri);
                indexXmlTag.RemoveLink(uri.DeleteSourceUris.XmlUri);

                any = true;
            }

            return any;
        }

        private async Task WriteIndexTags(SoftwareIdentity indexJsonTag, SoftwareIdentity indexXmlTag, CloudBlobDirectory sourceDirectory)
        {
            var blobJson = sourceDirectory.GetBlockBlobReference("index.json.SoftwareIdentity");

            var blobXml = sourceDirectory.GetBlockBlobReference("index.xml.SoftwareIdentity");

            var json = indexJsonTag.SwidTagJson;

            var xml = indexXmlTag.SwidTagXml;

            await Task.WhenAll(
                blobJson.UploadTextAsync(json),
                blobXml.UploadTextAsync(xml)
                );

            blobJson.Properties.CacheControl = "public, max-age=300"; // cache for 5 minutes.
            blobXml.Properties.CacheControl = "public, max-age=300"; // cache for 5 minutes.

            blobJson.Properties.ContentType = FearTheCowboy.Iso19770.Schema.MediaType.SwidTagJsonLd;
            blobXml.Properties.ContentType = FearTheCowboy.Iso19770.Schema.MediaType.SwidTagXml;

            await Task.WhenAll(
                blobJson.SetPropertiesAsync(),
                blobXml.SetPropertiesAsync()
                );
        }

        //private static Task<TagBlob> WriteActiveTag(CloudBlobContainer channelContainer, string tagBlobName, TagTransactionEntity tag)
        //{
        //    var activeTagBlobRelativeUri = AzureUris.JsonActiveTagBlobRelativeUri(tagBlobName);

        //    var versionedTagBlobRelativeUri = AzureUris.JsonTagVersionedBlobRelativeUri(tagBlobName, tag.Version);

        //    var blob = channelContainer.GetBlockBlobReference(activeTagBlobRelativeUri);

        //    var history = GetTagBlobHistory(blob);

        //    history.Add(new TagBlobHistory() { Id = versionedTagBlobRelativeUri, Version = tag.Version });

        //    var tagBlob = new TagBlob(blob.Uri, tag, history);

        //    return UploadTagToBlob(tagBlob, blob, tag.Fingerprint);
        //}

        private static async Task<TagBlobUris> WriteVersionedTag(CloudBlobDirectory sourceContainer, TagEntity tagEntity, SoftwareIdentity SoftwareIdentity)
        {
            var json = SoftwareIdentity.SwidTagJson;

            var xml = SoftwareIdentity.SwidTagXml;

            var jsonBlobUri = AzureUris.JsonTagVersionedBlobRelativeUri(tagEntity.TagAzid, tagEntity.Version);

            var xmlBlobUri = AzureUris.XmlTagVersionedBlobRelativeUri(tagEntity.TagAzid, tagEntity.Version);

            var jsonBlob = sourceContainer.GetBlockBlobReference(jsonBlobUri);

            var xmlBlob = sourceContainer.GetBlockBlobReference(xmlBlobUri);

            //var revisions = GetTagBlobRevisions(blob);

            await Task.WhenAll(
                UploadTagToBlob(tagEntity, jsonBlob, json, FearTheCowboy.Iso19770.Schema.MediaType.SwidTagJsonLd),
                UploadTagToBlob(tagEntity, xmlBlob, xml, FearTheCowboy.Iso19770.Schema.MediaType.SwidTagXml)
                );

            return new TagBlobUris() { JsonUri = jsonBlob.Uri, XmlUri = xmlBlob.Uri };
        }

        //private static List<TagBlobHistory> GetTagBlobHistory(CloudBlockBlob blob)
        //{
        //    var history = new List<TagBlobHistory>();

        //    if (blob.Exists())
        //    {
        //        var json = blob.DownloadText(Encoding.UTF8);

        //        var existingTag = JsonConvert.DeserializeObject<TagBlob>(json);

        //        if (existingTag.History != null)
        //        {
        //            history.AddRange(existingTag.History);
        //        }
        //    }
        //    return history;
        //}

        private static List<string> GetTagBlobRevisions(CloudBlockBlob blob)
        {
            List<string> revisions = null;

            // TODO: Bring this back
            //if (blob.Exists())
            //{
            //    var str = blob.DownloadText(Encoding.UTF8);

            //    var existingTag = JsonConvert.DeserializeObject<TagBlob>(str);

            //    var snapshot = blob.CreateSnapshot();

            //    revisions = new List<string>();

            //    revisions.Add(snapshot.SnapshotQualifiedUri.AbsoluteUri);

            //    if (existingTag.Revisions != null)
            //    {
            //        revisions.AddRange(existingTag.Revisions);
            //    }
            //}

            return revisions;
        }

        private async Task<string> GetBlobAsString(string blobUri)
        {
            var storage = this.Connection.ConnectToTagStorage();

            var blob = new CloudBlockBlob(new Uri(blobUri), storage.Credentials);

            return await blob.DownloadTextAsync();
        }

        private static IEnumerable<Task> CreateRedirects(CloudTable redirectsTable, IEnumerable<RedirectEntity> redirects)
        {
            foreach (var redirect in redirects)
            {
                var op = TableOperation.InsertOrReplace(redirect);

                yield return redirectsTable.ExecuteAsync(op);
            }
        }

        private class TagBlobUris
        {
            public Uri JsonUri { get; set; }

            public Uri XmlUri { get; set; }
        }

        private class AddedTagResult
        {
            public List<RedirectEntity> Redirects { get; set; }

            public TagEntity Tag { get; set; }
        }

        private class UpdatedTagResult
        {
            public TagBlobUris NewSourceUris { get; set; }

            public TagBlobUris OldSourceUris { get; set; }

            public List<RedirectEntity> Redirects { get; set; }

            public TagEntity Tag { get; set; }

            public TagEntity PrimaryTag { get; set; }
        }

        private class DeleteTagResult
        {
            public TagBlobUris DeleteSourceUris { get; set; }

            public TagEntity Tag { get; set; }
        }
#endif
    }
}
