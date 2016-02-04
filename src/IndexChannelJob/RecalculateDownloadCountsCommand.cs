using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AppSyndication.WebJobs.Data;

namespace AppSyndication.WebJobs.IndexChannelJob
{
    public class RecalculateDownloadCountsCommand
    {
        public RecalculateDownloadCountsCommand(Connection connection)
        {
            this.Connection = connection;
        }

        private Connection Connection { get; }

        public bool DidWork { get; private set; }

        public async Task<bool> ExecuteAsync()
        {
            this.DidWork = false;

            Console.WriteLine("Recalculating download counts.");

            //var txTable = new TagTransactionTable(this.Connection);

            //var txInfo = txTable.GetTransactionInfo();

            var txInfo = this.Connection.TransactionTable().GetSystemInfo();

            //var storage = this.Connection.ConnectToTagStorage();

            //var tables = storage.CreateCloudTableClient();

            //var tagsTable = new TagsTable(tables);

            //var redirectsTable = new DownloadRedirectsTable(tables);

            var redirectsTable = this.Connection.DownloadRedirectsTable();

            var downloadsTable = this.Connection.DownloadsTable(); //tables.GetTableReference("downloads");

            //await downloadsTable.CreateIfNotExistsAsync();

            var downloads = downloadsTable.GetDownloadsSince(txInfo.LastRecalculatedDownloadCount);
#if false
            var lastTime = txInfo.LastRecalculatedDownloadCount ?? AzureDateTime.Min;
            //lastTime = lastTime.Subtract(new TimeSpan(1, 0, 0));

            var newLastTime = DateTime.UtcNow;
            //var newLastTime = DateTime.UtcNow.Add(new TimeSpan(1, 0, 0));

            var startTime = lastTime.ToString("yyyy-MM-ddTHH-mm}");

            var endTime = newLastTime.ToString("yyyy-MM-ddTHH-mm{");

            var filter = TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, startTime),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, endTime));

            var query = new TableQuery<DownloadEntity>().Where(filter);
#endif

            //var updates = CalculateDownloadCounts(redirectsTable, downloadsTable, query);

            var updates = GatherDownloadCounts(redirectsTable, downloads);

            //if (updates.Any())
            //{
            //    this.DidWork = true;

            //    var updatedAzids = await UpdateVersionedTagCounts(tagsTable, redirectsTable, updates);

            //    await UpdatePrimaryTagCounts(tagsTable, updatedAzids);

            //    txInfo.LastRecalculatedDownloadCount = newLastTime;

            //    var txOp = TableOperation.Merge(txInfo);

            //    await txTable.Table.ExecuteAsync(txOp);

            //    Console.WriteLine("Download counts updated.");
            //}
            //else
            //{
            //    Console.WriteLine("No download counts required recalculation.");
            //}

            return this.DidWork;
        }


        private static IEnumerable<DownloadCount> GatherDownloadCounts(DownloadRedirectsTable redirectsTable, IEnumerable<DownloadEntity> downloads)
        {
            var countUpdates = new Dictionary<string, DownloadCount>();

            foreach (var download in downloads)
            {
                var redirect = redirectsTable.GetRedirect(download.DownloadKey);

                if (redirect != null)
                {
                    DownloadCount dc;

                    if (!countUpdates.TryGetValue(download.DownloadKey, out dc))
                    {
                        dc = new DownloadCount(redirect); //() { RedirectKey = redirect.PartitionKey, SourceAzid = redirect.SourceAzid, TagAzid = redirect.TagAzid, TagUid = redirect.TagUid, TagVersion = redirect.TagVersion };

                        countUpdates.Add(redirect.Id, dc);
                    }

                    // This protects against the case that the system transaction's download count last updated was not successfully
                    // written but the redirect count was updated.
                    if (redirect.DownloadCountLastUpdated.HasValue && redirect.DownloadCountLastUpdated < download.Timestamp)
                    {
                        ++dc.Count;
                    }
                }
            }

            return countUpdates.Values;
        }

#if false
        private static IEnumerable<DownloadCount> CalculateDownloadCounts(DownloadRedirectsTable redirectsTable, CloudTable downloadsTable, TableQuery<DownloadEntity> query)
        {
            var countUpdates = new Dictionary<string, DownloadCount>();

            foreach (var download in downloadsTable.ExecuteQuery(query))
            {
                var redirect = redirectsTable.GetRedirect(download.DownloadKey);

                if (redirect != null)
                {
                    DownloadCount dc;

                    if (!countUpdates.TryGetValue(download.DownloadKey, out dc))
                    {
                        dc = new DownloadCount() { RedirectKey = redirect.PartitionKey, SourceAzid = redirect.SourceAzid, TagAzid = redirect.TagAzid, TagUid = redirect.TagUid, TagVersion = redirect.TagVersion };

                        countUpdates.Add(dc.RedirectKey, dc);
                    }

                    ++dc.Count;
                }
            }

            return countUpdates.Values;
        }
#endif

        private static async Task<IEnumerable<string>> UpdateVersionedTagCounts(TagsTable tagsTable, DownloadRedirectsTable redirectsTable, IEnumerable<DownloadCount> updates)
        {
            var foundRedirects = new Dictionary<string, DownloadRedirectEntity>();
            var foundTags = new Dictionary<string, TagEntity>();

            var tasks = new List<Task>();

            var tagUids = new List<string>();

            //// Update the versioned entities with the correct download counts.
            //foreach (var dc in updates)
            //{
            //    //DownloadRedirectEntity redirect;

            //    //if (!foundRedirects.TryGetValue(dc.RedirectKey, out redirect))
            //    //{
            //    //    redirect = redirectsTable.GetRedirect(dc.RedirectKey);

            //    //    foundRedirects.Add(dc.RedirectKey, redirect);
            //    //}

            //    var tagKey = String.Join("|", dc.SourceAzid, dc.TagAzid, dc.TagVersion);

            //    TagEntity tag;

            //    if (!foundTags.TryGetValue(tagKey, out tag))
            //    {
            //        tag = tagsTable.GetTag(dc.SourceAzid, dc.TagAzid, dc.TagVersion);

            //        foundTags.Add(tagKey, tag);
            //    }

            //    redirect.DownloadCount += dc.Count;

            //    tag.DownloadCount += dc.Count;
            //}

            //// Write the changes back to table storage.
            //foreach (var redirect in foundRedirects.Values)
            //{
            //    var redirectOp = TableOperation.Merge(redirect);

            //    var task = redirectsTable.Table.ExecuteAsync(redirectOp);

            //    tasks.Add(task);
            //}

            //foreach (var tag in foundTags.Values)
            //{
            //    var tagOp = TableOperation.Merge(tag);

            //    var task = tagsTable.Table.ExecuteAsync(tagOp);

            //    tasks.Add(task);

            //    tagUids.Add(tag.Uid);
            //}

            //await Task.WhenAll(tasks);

            // Return the set of uids that were updated.
            return tagUids;
        }

        ////private static async Task<IEnumerable<string>> UpdateVersionedTagCounts(TagsTable tagsTable, DownloadRedirectsTable redirectsTable, IEnumerable<DownloadCount> updates)
        ////{
        ////    var foundRedirects = new Dictionary<string, DownloadRedirectEntity>();
        ////    var foundTags = new Dictionary<string, TagEntity>();

        ////    var tasks = new List<Task>();

        ////    var tagUids = new List<string>();

        ////    // Update the versioned entities with the correct download counts.
        ////    foreach (var dc in updates)
        ////    {
        ////        DownloadRedirectEntity redirect;

        ////        if (!foundRedirects.TryGetValue(dc.RedirectKey, out redirect))
        ////        {
        ////            redirect = redirectsTable.GetRedirect(dc.RedirectKey);

        ////            foundRedirects.Add(dc.RedirectKey, redirect);
        ////        }

        ////        var tagKey = String.Join("|", dc.SourceAzid, dc.TagAzid, dc.TagVersion);

        ////        TagEntity tag;

        ////        if (!foundTags.TryGetValue(tagKey, out tag))
        ////        {
        ////            tag = tagsTable.GetTag(dc.SourceAzid, dc.TagAzid, dc.TagVersion);

        ////            foundTags.Add(tagKey, tag);
        ////        }

        ////        redirect.DownloadCount += dc.Count;

        ////        tag.DownloadCount += dc.Count;
        ////    }

        ////    // Write the changes back to table storage.
        ////    foreach (var redirect in foundRedirects.Values)
        ////    {
        ////        var redirectOp = TableOperation.Merge(redirect);

        ////        var task = redirectsTable.Table.ExecuteAsync(redirectOp);

        ////        tasks.Add(task);
        ////    }

        ////    foreach (var tag in foundTags.Values)
        ////    {
        ////        var tagOp = TableOperation.Merge(tag);

        ////        var task = tagsTable.Table.ExecuteAsync(tagOp);

        ////        tasks.Add(task);

        ////        tagUids.Add(tag.Uid);
        ////    }

        ////    await Task.WhenAll(tasks);

        ////    // Return the set of uids that were updated.
        ////    return tagUids;
        ////}

        ////private static async Task UpdatePrimaryTagCounts(TagsTable tagsTable, IEnumerable<string> updatedAzids)
        ////{
        ////    var tasks = new List<Task>();

        ////    foreach (var azid in updatedAzids)
        ////    {
        ////        var tags = tagsTable.GetAllTagsForAzid(azid).ToList();

        ////        var primaryTag = tags.Single(t => String.IsNullOrEmpty(t.RowKey));

        ////        primaryTag.DownloadCount = tags.Where(t => !String.IsNullOrEmpty(t.RowKey)).Sum(t => t.DownloadCount);

        ////        var op = TableOperation.Merge(primaryTag);

        ////        var task = tagsTable.Table.ExecuteAsync(op);

        ////        tasks.Add(task);
        ////    }

        ////    await Task.WhenAll(tasks);
        ////}

        private class DownloadCount
        {
            public DownloadCount(DownloadRedirectEntity redirect)
            {
                this.Redirect = redirect;
            }

            private DownloadRedirectEntity Redirect { get; }
            //public string RedirectKey { get; set; }

            //public string SourceAzid { get; set; }

            //public string TagAzid { get; set; }

            //public string TagUid { get; set; }

            //public string TagVersion { get; set; }

            public int Count { get; set; }
        }
    }
}
