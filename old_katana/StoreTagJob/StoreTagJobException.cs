using System;

namespace AppSyndication.WebJobs.StoreTagJob
{
    internal class StoreTagJobException : Exception
    {
        public StoreTagJobException(string message)
            : base(message)
        {
        }
    }
}
