using System;

namespace AppSyndication.WebJobs.IndexChannelJob
{
    internal class IndexChannelJobException : Exception
    {
        public IndexChannelJobException(string message)
            : base(message)
        {
        }
    }
}
