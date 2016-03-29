using System.Web.Configuration;

namespace AppSyndication.UploadService.Data
{
    public abstract class EnvironmentConfiguration
    {
        protected EnvironmentConfiguration()
        {
            this.Environment = WebConfigurationManager.AppSettings.Get("Environment") ?? "Dev";
        }

        public string Environment { get; }

        protected string GetConnectionString(string key, string defaultValue = null)
        {
            var environmentKey = key + "." + this.Environment;
            return WebConfigurationManager.ConnectionStrings[environmentKey]?.ConnectionString ?? defaultValue;
        }

        protected string GetSetting(string key, string defaultValue = null)
        {
            var environmentKey = key + "." + this.Environment;
            return WebConfigurationManager.AppSettings[environmentKey] ?? defaultValue;
        }
    }

    public class UploadServiceEnvironmentConfiguration : EnvironmentConfiguration
    {
        public string IdentityServerUrl => base.GetConnectionString("IdentityServerUrl");

        public string TableStorageConnectionString => base.GetConnectionString("Storage");
    }
}
