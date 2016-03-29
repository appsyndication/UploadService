using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Threading.Tasks;
using System.Web.Http;
using AppSyndication.UploadService.Data;

namespace AppSyndication.UploadService.WebSvc
{
    [Route("upload")]
    public class UploadApiController : ApiController
    {
        public UploadApiController(UploadServiceEnvironmentConfiguration environment)
        {
            this.Environment = environment;
        }

        private UploadServiceEnvironmentConfiguration Environment { get; }

        // GET upload
        public dynamic Get()
        {
            var user = this.User as ClaimsPrincipal;

            var claims = user?.Identities.FirstOrDefault()?.Claims;

            var data = claims?.Select(c => new { c.Type, c.Value });

            return data;
        }

        // POST upload
        public async Task<HttpResponseMessage> Post(string channel = null)
        {
            var user = this.User as ClaimsPrincipal;

            if (user == null)
            {
                return this.Request.CreateResponse(HttpStatusCode.Unauthorized);
            }

            var name = user.Identity.Name;

            var connection = new Connection(this.Environment.TableStorageConnectionString);

            var start = await StartTagTransaction.CreateAsync(connection, channel, name);

            //this.Request.Content.ReadAsFormDataAsync()

            if (this.Request.Content.IsMimeMultipartContent())
            {
                using (var stream = start.GetWriteStream())
                {
                    var provider = new MultipartBlobStreamProvider(stream);

                    await this.Request.Content.ReadAsMultipartAsync<MultipartBlobStreamProvider>(provider);

                    start.SetFilename(provider.Filename);
                }
            }
            else
            {
                using (var stream = await this.Request.Content.ReadAsStreamAsync())
                {
                    await start.WriteToStream(stream);
                }
            }

            await start.CompleteAsync();

            return this.Request.CreateResponse(HttpStatusCode.OK);
        }

        // PUT api/values/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/values/5
        public void Delete(int id)
        {
        }

        private class MultipartBlobStreamProvider : MultipartStreamProvider
        {
            public MultipartBlobStreamProvider(Stream writeBlobStream)
            {
                this.WriteBlobStream = writeBlobStream;
            }

            private Stream WriteBlobStream { get; }

            public string Filename { get; private set; }

            //public MultipartBlobStreamProvider(string channel, string username)
            //{
            //    var cs = WebConfigurationManager.ConnectionStrings["Data"];
            //    var connection = new Connection(cs.ConnectionString);
            //    this.Transactions = connection.TransactionTable();

            //    this.Channel = channel ?? "~";
            //    this.Username = username ?? "robmen";
            //}

            //private TransactionTable Transactions { get; }

            //private string Channel { get; }

            //private string Username { get; }

            /// <summary>
            /// Only supports getting a single stream.
            /// </summary>
            /// <param name="parent"></param>
            /// <param name="headers"></param>
            /// <returns></returns>
            public override Stream GetStream(HttpContent parent, HttpContentHeaders headers)
            {
                var filename = headers.ContentDisposition?.FileName;

                if (!String.IsNullOrEmpty(this.Filename) || String.IsNullOrEmpty(filename))
                {
                    return new MemoryStream();
                }

                this.Filename = filename.Trim('"');
                return this.WriteBlobStream;
            }
        }
    }
}
