using System;
using System.Collections.Generic;
using System.Linq;

namespace FireGiant.AppSyndication.Processing.Models
{
    public class TagBlob
    {
        public TagBlob() { }

        public TagBlob(Uri uri, TagTransactionEntity tag, IEnumerable<string> revisions)
        {
            this.Uri = uri;

            this.Fingerprint = tag.Fingerprint;

            if (revisions != null)
            {
                this.Revisions = revisions.ToArray();
            }
        }

        public Uri Uri { get; set; }

        public string Fingerprint { get; set; }

        public string[] Revisions { get; private set; }
    }
}
