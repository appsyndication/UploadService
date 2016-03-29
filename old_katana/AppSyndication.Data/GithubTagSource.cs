using System;

namespace FireGiant.AppSyndication.Data
{
    public class GithubTagSource : TagSource
    {
        public GithubTagSource(string owner, string repository, string branch)
        {
            this.Owner = owner;
            this.Repository = repository;
            this.Branch = branch;

            this.Type = TagSourceType.Github;
            this.Uri = String.Concat("https://github.com/", this.Owner, "/", this.Repository, "/tree/", this.Branch);
        }

        public string Owner { get; private set; }

        public string Repository { get; private set; }

        public string Branch { get; private set; }

        public static bool TryParse(string sourceUri, out GithubTagSource githubChannel)
        {
            githubChannel = null;

            if (sourceUri.StartsWith("https://github.com/", StringComparison.OrdinalIgnoreCase) ||
                sourceUri.StartsWith("http://github.com/", StringComparison.OrdinalIgnoreCase))
            {
                var endOwner = sourceUri.IndexOf("/", 19);
                var endRepository = sourceUri.IndexOf("/", endOwner + 1);
                var endTree = sourceUri.IndexOf("/", endRepository + 1);

                var owner = sourceUri.Substring(19, endOwner - 19);
                var repository = sourceUri.Substring(endOwner + 1, endRepository - endOwner - 1);
                var branch = endTree == -1 ? "master" : sourceUri.Substring(endTree + 1);

                githubChannel = new GithubTagSource(owner, repository, branch);
            }
            else if (sourceUri.StartsWith("github:||", StringComparison.Ordinal))
            {
                var endOwner = sourceUri.IndexOf("|", 9);
                var endRepository = sourceUri.IndexOf("|", endOwner + 1);

                var owner = sourceUri.Substring(9, endOwner - 9);
                var repository = sourceUri.Substring(endOwner + 1, endRepository - endOwner - 1);
                var branch = sourceUri.Substring(endRepository + 1);

                githubChannel = new GithubTagSource(owner, repository, branch);
            }

            return githubChannel != null;
        }
    }
}
