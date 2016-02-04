﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using System.Web.Configuration;
using System.Web.Http;
using AppSyndication.WebJobs.Data;

namespace Web.Controllers
{
    //[Authorize]
    public class ValuesController : ApiController
    {
        // GET api/values
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/values/5
        public string Get(int id)
        {
            return "value";
        }

        // POST api/values
        public async Task<HttpResponseMessage> Post(string channel = null)
        {
            var cs = WebConfigurationManager.ConnectionStrings["Data"];

            var connection = new Connection(cs.ConnectionString);

            var start = await StartTagTransaction.CreateAsync(connection, channel, "robmen");

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
