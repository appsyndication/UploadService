using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AppSyndication.BackendModel.Data;

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

            var txTable = this.Connection.TransactionTable();

            var txInfo = txTable.GetSystemInfo();

            var downloadTable = this.Connection.DownloadTable();

            var downloads = downloadTable.GetDownloadsSince(txInfo.LastRecalculatedDownloadCount).ToList();

            var redirectTable = this.Connection.RedirectTable();

            DateTime lastTime;

            var updates = GatherDownloadCounts(redirectTable, downloads, out lastTime).ToList();

            if (updates.Any())
            {
                var tagTable = this.Connection.TagTable();

                await UpdateTagDownloadCounts(tagTable, redirectTable, updates, lastTime);

                txInfo.LastRecalculatedDownloadCount = lastTime;

                await txTable.Update(txInfo);

                this.DidWork = true;
            }

            return this.DidWork;
        }

        private static IEnumerable<DownloadCount> GatherDownloadCounts(RedirectTable redirectsTable, IEnumerable<DownloadEntity> downloads, out DateTime lastTime)
        {
            var countUpdates = new Dictionary<string, DownloadCount>();

            lastTime = DateTime.MinValue;

            foreach (var download in downloads)
            {
                var redirect = redirectsTable.GetRedirect(download.DownloadKey);

                if (redirect != null)
                {
                    // This protects against the case that the system transaction's download count last updated was not successfully
                    // written but the redirect count was updated.
                    if (redirect.DownloadCountLastUpdated.HasValue && redirect.DownloadCountLastUpdated < download.Timestamp)
                    {
                        DownloadCount dc;

                        if (!countUpdates.TryGetValue(download.DownloadKey, out dc))
                        {
                            dc = new DownloadCount(redirect);

                            countUpdates.Add(redirect.Id, dc);
                        }

                        ++dc.Count;
                    }
                }

                lastTime = (lastTime < download.Timestamp.DateTime) ? download.Timestamp.DateTime : lastTime;
            }

            return countUpdates.Values;
        }

        private static async Task UpdateTagDownloadCounts(TagTable tagTable, RedirectTable redirectTable, IEnumerable<DownloadCount> updates, DateTime lastTime)
        {
            var countedRedirect = new List<RedirectEntity>();
            var foundTags = new Dictionary<string, TagEntity>();

            // Update the versioned entities with the correct download counts.
            foreach (var dc in updates)
            {
                TagEntity tag;
                var tagUid = TagEntity.CalculateUid(dc.Redirect.TagPartitionKey, dc.Redirect.TagRowKey);

                if (!foundTags.TryGetValue(tagUid, out tag))
                {
                    tag = await tagTable.GetTagAsync(dc.Redirect.TagPartitionKey, dc.Redirect.TagRowKey);

                    if (tag == null)
                    {
                        continue;
                    }

                    Debug.Assert(tagUid == tag.Uid);
                    foundTags.Add(tag.Uid, tag);
                }

                TagEntity primaryTag;
                var primaryTagUid = tag.AsPrimary().Uid;

                if (!foundTags.TryGetValue(tagUid, out primaryTag))
                {
                    primaryTag = await tagTable.GetPrimaryTagAsync(tag);

                    if (primaryTag == null)
                    {
                        continue;
                    }

                    Debug.Assert(primaryTagUid == primaryTag.Uid);
                    foundTags.Add(primaryTag.Uid, primaryTag);
                }

                dc.Redirect.DownloadCount += dc.Count;
                dc.Redirect.DownloadCountLastUpdated = lastTime;

                countedRedirect.Add(dc.Redirect);

                tag.DownloadCount += dc.Count;

                primaryTag.DownloadCount += dc.Count;
            }

            // Write the changes back to table storage.
            foreach (var redirect in countedRedirect)
            {
                await redirectTable.Update(redirect);
            }

            foreach (var tag in foundTags.Values)
            {
                await tagTable.Update(tag);
            }
        }

        private class DownloadCount
        {
            public DownloadCount(RedirectEntity redirect)
            {
                this.Redirect = redirect;
            }

            public RedirectEntity Redirect { get; }

            public int Count { get; set; }
        }
    }
}
