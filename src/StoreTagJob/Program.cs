using System;
using System.IO;
using System.Threading.Tasks;
using AppSyndication.BackendModel.Data;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.DependencyInjection;

namespace AppSyndication.WebJobs.StoreTagJob
{
    public static class Program
    {
        private const string x = "UseDevelopmentStorage=true;";

        public static string Env { get; set; }

        public static string TableStorageConnectionString { get; set; }

        private static IServiceProvider _provider;

        public static void Main(string[] args)
        {
            var settings = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddJsonFile("appsettings.personal.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            Env = (args.Length > 0) ? args[0] : settings["AppSynEnvironment"] ?? "Development";

            TableStorageConnectionString = settings["AppSynDataConnection"];

            var config = new JobHostConfiguration(TableStorageConnectionString);

            //if (config.IsDevelopment)
            if (String.Compare("Development", Env, StringComparison.OrdinalIgnoreCase) == 0)
            {
                config.UseDevelopmentSettings();
            }

            var services = new ServiceCollection();
            services.AddTagStorage(TableStorageConnectionString);
            services.AddTransient<UpdateStorageCommand>();
            services.AddTransient<StoreTagCommand>();

            _provider = services.BuildServiceProvider();

            var host = new JobHost(config);
            host.RunAndBlock();
        }

        public static async Task StoreTag([QueueTrigger(StorageName.TagTransactionQueue)] StoreTagMessage message, string channel, string transactionId, int dequeueCount, TextWriter log)
        {
            using (var scope = _provider.GetRequiredService<IServiceScopeFactory>().CreateScope())
            {
                try
                {
                    var command = scope.ServiceProvider.GetService<StoreTagCommand>();

                    await command.ExecuteAsync(channel, transactionId);
                }
                catch (StoreTagJobException e)
                {
                    try
                    {
                        if (!e.OnlyLog)
                        {
                            ITagTransactionTable txTable = scope.ServiceProvider.GetService<ITagTransactionTable>();

                            await txTable.AddTagTransactionErrorMessageAsync(channel, transactionId, e.Message);
                        }

                        await log.WriteLineAsync(e.Message);
                    }
                    catch (Exception exception)
                    {
                        await log.WriteLineAsync($"Failed to store message for {channel}/{transactionId}. Original message: {e.Message}. Exception: {exception.ToString()}");
                    }
                }


                //var connection = new Connection(TableStorageConnectionString /*_environment.TableStorageConnectionString*/);

                //var tagTxTable = connection.TransactionTable();

                //var tagTx = await tagTxTable.GetTagTransactionAsync(channel, transactionId);

                //if (tagTx == null)
                //{
                //    await log.WriteLineAsync($"Could not find transaction id: {transactionId} in channel: {channel}");
                //    return;
                //}

                //try
                //{
                //    var update = new UpdateStorageCommand(connection, tagTx);
                //    await update.ExecuteAsync();

                //    if (update.DidWork)
                //    {
                //        await connection.QueueIndexMessageAsync(new IndexChannelMessage(channel));
                //    }
                //}
                //catch (StoreTagJobException e)
                //{
                //    try
                //    {
                //        await tagTxTable.AddTagTransactionErrorMessageAsync(tagTx, e.Message);
                //    }
                //    catch (Exception exception)
                //    {
                //        await log.WriteLineAsync($"Failed to store message for {channel}/{transactionId}. Original message: {e.Message}. Exception: {exception.ToString()}");
                //    }
                //}
            }
        }
    }
}