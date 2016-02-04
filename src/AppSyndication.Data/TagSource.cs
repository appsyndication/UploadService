using System;

namespace FireGiant.AppSyndication.Data
{
    public enum TagSourceType
    {
        Unknown,
        AppSynFeed,
        Github,
        SwidtagFeed,
    }

    public abstract class TagSource
    {
        public TagSourceType Type { get; protected set; }

        public string Uri { get; protected set; }

        public static bool TryParse(string uri, out TagSource source)
        {
            source = null;

            GithubTagSource github;

            if (String.IsNullOrEmpty(uri))
            {
            }
            else if (GithubTagSource.TryParse(uri, out github))
            {
                source = github;
            }

            return source != null;
        }
    }
}
