using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AppSyndication.BackendModel.Data;
using Microsoft.AspNetCore.Http;

namespace WebSvc
{
    public class UploadHandler
    {
        public UploadHandler(ITagTransactionTable transactionTable, ITagTransactionBlobContainer container, ITagQueue queue)
        {
            this.TagTransactionTable = transactionTable;

            this.TagTransactionBlobContainer = container;

            this.TagQueue = queue;
        }

        private ITagTransactionTable TagTransactionTable { get; }

        private ITagTransactionBlobContainer TagTransactionBlobContainer { get; }

        private ITagQueue TagQueue { get; }

        public async Task ExecuteAsync(HttpContext context)
        {
            Trace.TraceInformation($"{context.Request.Method} {context.Request.Path}");

            if (context.Request.Path.Equals("/favicon.ico"))
            {
                return;
            }

            if (context.Request.Method == "POST")
            {
                var user = context.User.Identities.FirstOrDefault(i => i.IsAuthenticated);
                if (user == null)
                {
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                string channel;
                string alias;

                if (!TryParsePath(context.Request.Path.Value, out channel, out alias))
                {
                    context.Response.StatusCode = StatusCodes.Status400BadRequest;
                    return;
                }

                var tx = await StartAsync(user, channel, alias);

                var redirect = await UploadFromRequest(context.Request, tx);

                await this.CompleteAsync(tx);

                if (String.IsNullOrEmpty(redirect))
                {
                    context.Response.StatusCode = StatusCodes.Status201Created;
                }
                else
                {
                    context.Response.Redirect(redirect);
                }

                return;
            }
            else
            {
                Trace.TraceInformation("Trying to get identity...");

                var identity = context.User?.Identities?.FirstOrDefault(i => i.IsAuthenticated);
                if (identity == null)
                {
                    Trace.TraceInformation("No identity, user needs to be logged in...");

                    //await context.Authentication.ChallengeAsync(OpenIdConnectDefaults.AuthenticationScheme, new AuthenticationProperties { RedirectUri = context.Request.Path.Value });
                    context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                Trace.TraceInformation("Found identity, user must be logged in...");

                var text = $"<html><head><title>Claims</title></head><body><h1>NameType: {identity.NameClaimType} Name: {identity.Name}</h1><table style='border: 1px solid black'><th><td>Issue</td><td>Type</td><td>Value</td></th>";
                foreach (var claim in identity.Claims)
                {
                    text += $"<tr><td>{claim.Issuer}</td><td>{claim.Type}</td><td>{claim.Value}</td></tr>";
                }
                text += "</table></body></html>";

                await context.Response.WriteAsync(text);

                return;
            }
        }

        private async Task<TagTransactionEntity> StartAsync(System.Security.Claims.ClaimsIdentity user, string channel, string alias)
        {
            var tx = new TagTransactionEntity(TagTransactionOperation.Create, channel, alias, user.Name);

            await this.TagTransactionTable.Create(tx);

            return tx;
        }

        private async Task<string> UploadFromRequest(HttpRequest request, TagTransactionEntity tx)
        {
            string redirect = null;

            if (request.ContentType == "multipart/form-data")
            {
                var form = await request.ReadFormAsync();

                redirect = form["redirectUri"];

                var file = form.Files[0];

                using (var stream = file.OpenReadStream())
                {
                    await this.TagTransactionBlobContainer.UploadFromStreamAsync(tx, stream);
                }
            }
            else
            {
                using (var stream = request.Body)
                {
                    await this.TagTransactionBlobContainer.UploadFromStreamAsync(tx, stream);
                }
            }

            return redirect;
        }

        private async Task CompleteAsync(TagTransactionEntity tx)
        {
            //await start.CompleteAsync();

            tx.Stored = DateTime.UtcNow;

            await this.TagTransactionTable.Update(tx);

            var message = new StoreTagMessage(tx.Channel, tx.Id);

            await this.TagQueue.EnqueueMessageAsync(message);
        }

        private bool TryParsePath(string path, out string channel, out string alias)
        {
            var split = path.IndexOf('/', 1); // skip the starting slash.

            if (split > 0)
            {
                channel = path.Substring(1, split);
                alias = path.Substring(split + 1);
            }
            else
            {
                channel = null;
                alias = path.Substring(1);
            }

            return !String.IsNullOrEmpty(alias) && !alias.Contains('/');
        }
    }
}