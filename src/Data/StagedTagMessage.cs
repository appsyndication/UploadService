
namespace AppSyndication.WebJobs.Data
{
    public class StagedTagMessage
    {
        public StagedTagMessage(string channel, string transactionId)
        {
            this.Channel = channel;
            this.TransactionId = transactionId;
        }

        public string Channel { get; set; }

        public string TransactionId { get; set; }
    }
}