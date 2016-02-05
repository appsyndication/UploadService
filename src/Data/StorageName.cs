namespace AppSyndication.WebJobs.Data
{
    public static class StorageName
    {
        public const string DownloadTable = "download";

        public const string IndexQueue = "tag-index-queue";

        public const string RedirectTable = "redirect";

        public const string SearchIndexBlobContainer = "search-index";

        public const string TagBlobContainer = "tag";

        public const string TagTable = "tag";

        public const string TagTransactionBlobContainer = "tagtx";

        public const string TagTransactionQueue = "tag-queue";

        public const string TransactionTable = "tagtx";
    }
}
