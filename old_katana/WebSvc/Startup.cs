using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens;
using System.Reflection;
using System.Threading.Tasks;
using System.Web.Helpers;
using System.Web.Http;
using AppSyndication.UploadService.Data;
using Autofac;
using Autofac.Integration.WebApi;
using IdentityModel;
using IdentityServer3.AccessTokenValidation;
using Microsoft.Owin;
using Microsoft.Owin.Security.Cookies;
using Microsoft.Owin.Security.OAuth;
using Owin;

//[assembly: OwinStartup(typeof(Startup))]

namespace AppSyndication.UploadService.WebSvc
{
    public class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            AntiForgeryConfig.UniqueClaimTypeIdentifier = JwtClaimTypes.Subject;
            JwtSecurityTokenHandler.InboundClaimTypeMap = new Dictionary<string, string>();

            var environment = new UploadServiceEnvironmentConfiguration();
            var builder = new ContainerBuilder();

            // Accept access tokens from identityserver and require a scope of 'upload'.
            //
            app.UseIdentityServerBearerTokenAuthentication(new IdentityServerBearerTokenAuthenticationOptions
            {
                Authority = environment.IdentityServerUrl,
                ValidationMode = ValidationMode.Local,
                TokenProvider = new FormBasedTokenProvider(),

                RequiredScopes = new[] { "upload" }
            });

            app.UseCookieAuthentication(new CookieAuthenticationOptions
            {
                AuthenticationType = "Cookies",
                CookieName = "as-id"
            });

            //var options = new OpenIdConnectAuthenticationOptions
            //{
            //    Authority = environment.IdentityServerUrl,
            //    ClientId = "as-up-web",
            //    RedirectUri = environment.RedirectUri,
            //    Scope = "openid upload",
            //    SignInAsAuthenticationType = "Cookies",
            //};

            var config = new HttpConfiguration();
            config.MapHttpAttributeRoutes();
            config.Filters.Add(new AuthorizeAttribute()); // require authentication for all controllers

            builder.RegisterApiControllers(Assembly.GetExecutingAssembly());
            builder.Register(context => environment);

            var container = builder.Build();
            config.DependencyResolver = new AutofacWebApiDependencyResolver(container);

            app.UseAutofacMiddleware(container);
            app.UseAutofacWebApi(config);
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
