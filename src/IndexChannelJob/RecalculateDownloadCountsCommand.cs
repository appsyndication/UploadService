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
        public RecalculateDownloadCountsCommand(ITagTransactionTable txTable, ITagTable tagTable, IRedirectTable redirectTable, IDownloadTable downloadTable)
        {
            this.TagTransactionTable = txTable;

            this.TagTable = tagTable;

            this.RedirectTable = redirectTable;

            this.DownloadTable = downloadTable;
        }

        private ITagTransactionTable TagTransactionTable { get; }

        private ITagTransactionBlobContainer TagTransactionContainer { get; }

        private ITagTable TagTable { get; }

        private ITagBlobContainer TagBlobContainer { get; }

        private IRedirectTable RedirectTable { get; }

        private IDownloadTable DownloadTable { get; }

        public bool DidWork { get; private set; }

        public async Task<bool> ExecuteAsync()
        {
            this.DidWork = false;

            var txInfo = this.TagTransactionTable.GetSystemInfo();

            var downloads = this.DownloadTable.GetDownloadsSince(txInfo.LastRecalculatedDownloadCount).ToList();

            DateTime lastTime;

            var updates = this.GatherDownloadCounts(downloads, out lastTime).ToList();

            if (updates.Any())
            {
                await UpdateTagDownloadCounts(updates, lastTime);

                txInfo.LastRecalculatedDownloadCount = lastTime;

                await this.TagTransactionTable.Update(txInfo);

                this.DidWork = true;
            }

            return this.DidWork;
        }

        private IEnumerable<DownloadCount> GatherDownloadCounts(IEnumerable<DownloadEntity> downloads, out DateTime lastTime)
        {
            var countUpdates = new Dictionary<string, DownloadCount>();

            lastTime = DateTime.MinValue;

            foreach (var download in downloads)
            {
                var redirect = this.RedirectTable.GetRedirect(download.DownloadKey);

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

        private async Task UpdateTagDownloadCounts(IEnumerable<DownloadCount> updates, DateTime lastTime)
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
                    tag = await this.TagTable.GetTagAsync(dc.Redirect.TagPartitionKey, dc.Redirect.TagRowKey);

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
                    primaryTag = await this.TagTable.GetPrimaryTagAsync(tag);

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
                await this.RedirectTable.Update(redirect);
            }

            foreach (var tag in foundTags.Values)
            {
                await this.TagTable.Update(tag);
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
