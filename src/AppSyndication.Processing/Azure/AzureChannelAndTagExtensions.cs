using FireGiant.AppSyndication.Data;

namespace FireGiant.AppSyndication.Processing.Azure
{
    public static class AzureChannelAndTagExtensions
    {
        public static string AzureId(this Channel channel)
        {
            return AzureUris.AzureSafeId(channel.Id);
        }

        public static string AzureId(this TagSource source)
        {
            return AzureUris.AzureSafeId(source.Uri);
        }

    }
}
