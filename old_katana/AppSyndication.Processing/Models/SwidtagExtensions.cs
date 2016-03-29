using FearTheCowboy.Iso19770;
using FireGiant.AppSyndication.Processing.Azure;

namespace FireGiant.AppSyndication.Processing.Models
{
    public static class SwidtagExtensions
    {
        //private const int MaxDescription = 30 * 1024;

        public static string AzureId(this SoftwareIdentity swidtag)
        {
            return AzureUris.AzureSafeId(swidtag.TagId);
        }

        //public static TagEntity AsTagEntity(this Swidtag swidtag, string sourceAzid, string fingerprint)
        //{
        //    var tagEntity = new TagEntity();

        //    tagEntity.PartitionKey = sourceAzid;
        //    tagEntity.RowKey = swidtag.AzureId();
        //    tagEntity.Fingerprint = fingerprint;

        //    tagEntity.Alias = swidtag.Name;
        //    tagEntity.TagId = swidtag.TagId;
        //    tagEntity.Version = swidtag.Version;

        //    //var meta = swidtag.Meta.FirstOrDefault();

        //    //if (meta != null)
        //    //{
        //    //    tagEntity.Description = meta.Description.Substring(0, Math.Min(meta.Description.Length, MaxDescription));

        //    //    tagEntity.Name = meta["title"];
        //    //    tagEntity.Keywords = meta["keyword"];
        //    //}

        //    //var links
        //    //tagEntity.ImageUri = link["logoUri"];

        //    return tagEntity;
        //}
    }
}
