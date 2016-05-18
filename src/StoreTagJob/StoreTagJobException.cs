using System;

namespace AppSyndication.WebJobs.StoreTagJob
{
    internal class StoreTagJobException : Exception
    {
        public StoreTagJobException(string message, bool onlyLog = false)
            : base(message)
        {
            this.OnlyLog = onlyLog;
        }

        public bool OnlyLog { get; private set; }
    }
}
