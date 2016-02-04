using Microsoft.WindowsAzure.Storage;

namespace FireGiant.AppSyndication.Processing
{
    public class Connection
    {
        public Connection(string tagStorageConnectionString, string indexStorageConnectionString)
        {
            this.TagStorageConnectionString = tagStorageConnectionString;
            this.IndexStorageConnectionString = indexStorageConnectionString;
        }

        private string TagStorageConnectionString { get; set; }

        public string IndexStorageConnectionString { get; set; }

        private CloudStorageAccount TagStorage { get; set; }

        private CloudStorageAccount IndexStorage { get; set; }

        public CloudStorageAccount ConnectToTagStorage()
        {
            if (this.TagStorage == null)
            {
                this.TagStorage = CloudStorageAccount.Parse(this.TagStorageConnectionString);
            }

            return this.TagStorage;
        }

        public CloudStorageAccount ConnectToIndexStorage()
        {
            if (this.IndexStorage == null)
            {
                this.IndexStorage = CloudStorageAccount.Parse(this.IndexStorageConnectionString);
            }

            return this.IndexStorage;
        }
    }
}
