
namespace FireGiant.AppSyndication.Data
{
    public enum ProcessActionType
    {
        Ingest,
        UpdateStorage,
        RecalculateDownloadCounts,
        Index,
    }

    public class ProcessAction
    {
        public ProcessActionType Action { get; set; }

        public string TagSourceUri { get; set; }
    }
}
