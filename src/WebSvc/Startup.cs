using System;
using System.Diagnostics;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using AppSyndication.BackendModel.Data;
using Microsoft.AspNet.Authentication.Cookies;
using Microsoft.AspNet.Builder;
using Microsoft.AspNet.Hosting;
using Microsoft.AspNet.Http;
using Microsoft.AspNet.Diagnostics;
using Microsoft.AspNet.Http.Features;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace WebSvc
{
    public class Startup
    {
        public Startup()
        {
            // Set up configuration sources.
            this.Configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables()
                .Build();
        }

        public IConfigurationRoot Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit http://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            var connectionString = this.Configuration["AppSynDataConnection"];

            services.AddAuthentication(sharedOptions =>
                sharedOptions.SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme);

            services.AddTagStorage(connectionString);

            services.AddTransient<UploadHandler>();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, ILoggerFactory loggerFactory)
        {
            Trace.Listeners.Add(new AzureApplicationLogTraceListener());
            loggerFactory.AddConsole(this.Configuration.GetSection("Logging"));
            loggerFactory.AddDebug(LogLevel.Verbose);

            JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Clear();

            app.UseIISPlatformHandler();

            // app.UseDeveloperExceptionPage();

            // Configure the error handler to show an error page.
            app.UseExceptionHandler(errorApp =>
            {
                Trace.TraceInformation("Planning to run error app...");

                errorApp.Run(async context =>
                {
                    Trace.TraceInformation("Running error app...");

                    context.Response.StatusCode = 500;
                    context.Response.ContentType = "text/plain";

                    var error = context.Features.Get<IExceptionHandlerFeature>();
                    if (error != null)
                    {
                        Trace.TraceError(error.Error.Message);

                        // This error information would not normally be exposed to the client
                        await context.Response.WriteAsync("Error: " + error.Error.Message + "\r\n");
                    }

                    await context.Response.WriteAsync(new string(' ', 512)); // Padding for IE
                });
            });

            app.UseCookieAuthentication(options =>
            {
                options.AutomaticAuthenticate = true;
            });

            //var options = new OpenIdConnectOptions
            //{
            //    AutomaticChallenge = true,
            //    Authority = this.Configuration["AppSynOidcAuthority"],
            //    ClientId = this.Configuration["AppSynOidcClientId"],
            //    ClientSecret = this.Configuration["AppSynOidcClientSecret"],

            //    ResponseType = OpenIdConnectResponseTypes.Code,
            //    GetClaimsFromUserInfoEndpoint = true,

            //    //SignInScheme = CookieAuthenticationDefaults.AuthenticationScheme,
            //    //PostLogoutRedirectUri = "/logout",
            //    //CallbackPath = new PathString("/cb"),
            //    //RequireHttpsMetadata = false,
            //    //Events = new OpenIdConnectEvents
            //    //{
            //    //}
            //    Scope = { "openid", "profile", "upload" },
            //    TokenValidationParameters.NameClaimType = "sub",
            //};
            //app.UseOpenIdConnectAuthentication(options);

            app.UseOpenIdConnectAuthentication(options =>
            {
                options.AutomaticChallenge = true;

                options.Authority = this.Configuration["AppSynOidcAuthority"];
                options.ClientId = this.Configuration["AppSynOidcClientId"];
                options.ClientSecret = this.Configuration["AppSynOidcClientSecret"];

                options.ResponseType = OpenIdConnectResponseTypes.Code;
                options.GetClaimsFromUserInfoEndpoint = true;

                options.Scope.Add("upload");
                options.TokenValidationParameters.NameClaimType = "sub";
            });

            app.Run(async context =>
            {
                using (var scope = context.RequestServices.GetRequiredService<IServiceScopeFactory>().CreateScope())
                {
                    var controller = scope.ServiceProvider.GetRequiredService<UploadHandler>();

                    await controller.ExecuteAsync(context);
                }
            });
        }

        public static void Main(string[] args) => WebApplication.Run<Startup>(args);
    }
}
