using System;
using System.Linq;
using FireGiant.AppSyndication.Data;
using Microsoft.PackageManagement.SwidTag;
using Xunit;

namespace FireGiantTest.ApplicationSyndication.WebJob
{
    public class GithubTagDomainFixture
    {
        [Fact]
        public void CanParseSimple()
        {
            GithubTagSource gtd;

            Assert.True(GithubTagSource.TryParse("github:||appsyndication|test|master", out gtd));
            Assert.Equal("appsyndication", gtd.Owner);
            Assert.Equal("test", gtd.Repository);
            Assert.Equal("master", gtd.Branch);
        }

        [Fact]
        public void CanParseUri()
        {
            var uri = new Uri("http://ap.core.net/abc/123@v2.1.2.jsontag");

            var uri2 = new Uri("http://ap.core.net/a/http://ap.core.net/abc/123@v2.1.2.jsontag");

            Assert.Equal("/abc/123@v2.1.2.jsontag", uri.PathAndQuery);
        }

        [Fact]
        public void Foo()
        {
            var src = @"{
  ""link"": {
    ""http://wixtoolset.org/downloads/v4.0.2220.0/wix40.exe"": {
      ""rel"": ""installationmedia""
    },
    ""http://wixtoolset.org/logo.png"": {
      ""rel"": ""logo""
    }
  },
  ""meta"": {
    ""title"": ""WiX Toolset v4"",
    ""keyword"" : ""wix""
  },
  ""name"": ""wix4"",
  ""tagId"": ""http://wixtoolset.org/releases/wix4/"",
  ""version"": ""4.0.2220.0"",
  ""@context"": ""http://packagemanagement.org/discovery""
}";
            var tag = Swidtag.LoadJson(src);

            var meta = tag.Meta.First();
            var title = meta["title"];
            Assert.Equal("WiX Toolset v4", title);

            var keyword = meta["keyword"];
            Assert.Equal("wix", keyword);

            var links = tag.Links.ToList();
            Assert.Equal("installationmedia", links[0].Relationship);
            Assert.Equal("logo", links[1].Relationship);
        }
    }
}
