using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using System.Web.Helpers;
using System.Web.Http;
using IdentityModel;
using IdentityServer3.AccessTokenValidation;
using Microsoft.Owin;
using Microsoft.Owin.Security;
using Microsoft.Owin.Security.Cookies;
using Microsoft.Owin.Security.OAuth;
using Owin;

[assembly: OwinStartup(typeof(Web.Startup))]

namespace Web
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            AntiForgeryConfig.UniqueClaimTypeIdentifier = JwtClaimTypes.Subject;
            JwtSecurityTokenHandler.InboundClaimTypeMap = new Dictionary<string, string>();

            // Accept access tokens from identityserver and require a scope of 'upload'.
            //
            app.UseIdentityServerBearerTokenAuthentication(new IdentityServerBearerTokenAuthenticationOptions
            {
                Authority = "https://localhost:44301",
                ValidationMode = ValidationMode.Local,
                TokenProvider = new FormBasedTokenProvider(),

                RequiredScopes = new[] { "upload" }
            });

            app.UseCookieAuthentication(new CookieAuthenticationOptions
            {
                AuthenticationType = "Cookies",
                CookieName = "as-id"
            });

            var config = new HttpConfiguration();
            config.MapHttpAttributeRoutes();
            config.Filters.Add(new AuthorizeAttribute()); // require authentication for all controllers

            app.UseWebApi(config);
        }
    }

    public class FormBasedTokenProvider : IOAuthBearerAuthenticationProvider
    {
        public Task RequestToken(OAuthRequestTokenContext context)
        {
            if (String.IsNullOrEmpty(context.Token))
            {
                //var formdata = await context.Request.ReadFormAsync();
                //context.Token = formdata.FirstOrDefault(f => f.Key == "token").Value?.FirstOrDefault();

                if (String.IsNullOrEmpty(context.Token))
                {
                    //context.Token = context.Request.Cookies["idsrv"];
                }
            }

            return Task.FromResult(0);
        }

        public Task ValidateIdentity(OAuthValidateIdentityContext context)
        {
            context.Validated();

            return Task.FromResult(0);
        }

        public Task ApplyChallenge(OAuthChallengeContext context)
        {
            return Task.FromResult(0);
        }
    }
}
