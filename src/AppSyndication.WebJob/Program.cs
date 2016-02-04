using System;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using FireGiant.AppSyndication.Data;
using FireGiant.AppSyndication.Processing;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Serialization;

namespace FireGiant.AppSyndication.WebJob
{
    public class Program
    {
        public static Regex Fix = new Regex(@"[^A-Za-z0-9]+", RegexOptions.CultureInvariant | RegexOptions.ExplicitCapture | RegexOptions.Singleline);

        public bool Canceled { get; set; }

        public static int Main(string[] args)
        {
            JsonConvert.DefaultSettings = () => new JsonSerializerSettings() { ContractResolver = new CamelCasePropertyNamesContractResolver(), NullValueHandling = NullValueHandling.Ignore };

            // Parse the command line and if there are any errors, bail.
            //
            var commandLine = CommandLine.Parse(args);

            if (commandLine.Errors.Any())
            {
                foreach (var error in commandLine.Errors)
                {
                    Console.WriteLine(error);
                }

                return -2;
            }

            try
            {
                var result = 0;

                switch (commandLine.Command)
                {
                    case ProcessingCommand.Continuous:
                    case ProcessingCommand.Triggered:
                        AsyncPump.Run(async delegate { result = await new Program().ProcessQueue(commandLine.Command == ProcessingCommand.Continuous); });
                        break;

                    case ProcessingCommand.Ingest:
                        AsyncPump.Run(async delegate { result = await new Program().Ingest(commandLine.Source) ? 0 : 1; });
                        break;

                    case ProcessingCommand.Store:
                        AsyncPump.Run(async delegate { result = await new Program().UpdateStorage(commandLine.Source) ? 0 : 1; });
                        break;

                    case ProcessingCommand.Recalculate:
                        AsyncPump.Run(async delegate { result = await new Program().RecalculateDownloadCounts() ? 0 : 1; });
                        break;

                    case ProcessingCommand.Index:
                        AsyncPump.Run(async delegate { result = await new Program().Index() ? 0 : 1; });
                        break;
                }

                return result;
            }
            catch (StorageException e)
            {
                Console.Error.WriteLine("Unhandled Storage Exception: {0}", e.Message);
                Console.Error.WriteLine(e.RequestInformation.ExtendedErrorInformation.ErrorMessage);
                Console.Error.WriteLine("Stack trace: {0}: ", e.StackTrace);
            }
            finally
            {
                Console.WriteLine("Done.");
            }

            return -1;
        }

        public async Task<int> ProcessQueue(bool continuous)
        {
            var minWait = 1;

            var maxWait = 10;

            var wait = minWait;

            Trace.TraceInformation("Initializing connection to queue and creating if necessary.");

            var storageConnectionString = ConfigurationManager.ConnectionStrings["storage"].ConnectionString;

            var storage = CloudStorageAccount.Parse(storageConnectionString);

            var client = storage.CreateCloudQueueClient();

            var queue = client.GetQueueReference("tag-queue");

            await queue.CreateIfNotExistsAsync();

            do
            {
                //Trace.TraceInformation("Checking for message in the queue...");
                Console.WriteLine("Checking for message in the queue...");

                var message = queue.GetMessage(TimeSpan.FromMinutes(5));

                if (message == null)
                {
                    //Trace.TraceInformation("No message going to sleep for {0} seconds.", wait);
                    Console.WriteLine("No message going to sleep for {0} seconds.", wait);

                    Thread.Sleep(TimeSpan.FromSeconds(wait));

                    wait = Math.Min(maxWait, wait * 2);
                }
                else
                {
                    Trace.TraceInformation("Processing message.");

                    var deleteMessage = true;

                    var action = JsonConvert.DeserializeObject<ProcessAction>(message.AsString);

                    if (action.Action == ProcessActionType.Ingest || action.Action == ProcessActionType.UpdateStorage)
                    {
                        TagSource tagSource;

                        if (TagSource.TryParse(action.TagSourceUri, out tagSource))
                        {
                            switch (action.Action)
                            {
                                case ProcessActionType.Ingest:
                                    deleteMessage = await this.Ingest(tagSource);
                                    break;

                                case ProcessActionType.UpdateStorage:
                                    deleteMessage = await this.UpdateStorage(tagSource);
                                    break;
                            }
                        }
                        else
                        {
                            Trace.TraceError("Unknown tag source: {0}", action.TagSourceUri ?? "(null)");
                        }
                    }
                    else if (action.Action == ProcessActionType.RecalculateDownloadCounts || action.Action == ProcessActionType.Index)
                    {
                        switch (action.Action)
                        {
                            case ProcessActionType.RecalculateDownloadCounts:
                                deleteMessage = await this.RecalculateDownloadCounts();
                                break;

                            case ProcessActionType.Index:
                                deleteMessage = await this.Index();
                                break;
                        }
                    }
                    else
                    {
                        Trace.TraceError("Message contains unknown action type: {0}", action.Action);
                    }

                    if (deleteMessage)
                    {
                        Trace.TraceInformation("Completing removal of message from queue.");

                        await queue.DeleteMessageAsync(message);
                    }
                    else
                    {
                        Trace.TraceInformation("Skipping removal of message from queue since it was not yet processed.");
                    }

                    wait = minWait;

                    Trace.TraceInformation("Successfully processed message.");
                }
            } while (continuous && !this.Canceled);

            return 0;
        }

        public async Task<bool> Ingest(TagSource source)
        {
            Trace.TraceInformation("Ingesting tag source: {0}.", source.Uri);

            var storageConnectionString = ConfigurationManager.ConnectionStrings["storage"].ConnectionString;

            var searchAdminKey = ConfigurationManager.ConnectionStrings["search"].ConnectionString;

            var connection = new Connection(storageConnectionString, searchAdminKey);

            var command = new IngestTagsCommand();
            command.Connection = connection;
            command.Source = source;
            await command.ExecuteAsync();

            if (command.DidWork)
            {
                await this.QueueTagSource(ProcessActionType.UpdateStorage, source);
            }

            return true;
        }

        public async Task<bool> UpdateStorage(TagSource source)
        {
            Trace.TraceInformation("Updating storage with tag source: {0}.", source.Uri);

            var storageConnectionString = ConfigurationManager.ConnectionStrings["storage"].ConnectionString;

            var searchAdminKey = ConfigurationManager.ConnectionStrings["search"].ConnectionString;

            var connection = new Connection(storageConnectionString, searchAdminKey);

            var command = new UpdateStorageCommand();
            command.Connection = connection;
            command.Source = source;
            await command.ExecuteAsync();

            if (command.DidWork)
            {
                await this.QueueTagSource(ProcessActionType.Index, source);
            }

            return true;
        }

        public async Task<bool> RecalculateDownloadCounts()
        {
            Trace.TraceInformation("Recalculating download counts");

            var storageConnectionString = ConfigurationManager.ConnectionStrings["storage"].ConnectionString;

            var searchAdminKey = ConfigurationManager.ConnectionStrings["search"].ConnectionString;

            var connection = new Connection(storageConnectionString, searchAdminKey);

            var command = new RecalculateDownloadCountsCommand();
            command.Connection = connection;
            await command.ExecuteAsync();

            if (command.DidWork)
            {
                await this.QueueTagSource(ProcessActionType.Index, null);
            }

            return true;
        }

        public async Task<bool> Index()
        {
            Trace.TraceInformation("Indexing");

            var storageConnectionString = ConfigurationManager.ConnectionStrings["storage"].ConnectionString;

            var searchAdminKey = ConfigurationManager.ConnectionStrings["search"].ConnectionString;

            var connection = new Connection(storageConnectionString, searchAdminKey);

            var command = new IndexTagsCommand();
            command.Connection = connection;
            await command.ExecuteAsync();

            return true;
        }

        private async Task QueueTagSource(ProcessActionType action, TagSource source)
        {
            Trace.TraceInformation("Queueing work from tag source: {0}", (source == null) ? "(none)" : source.Uri);

            var account = CloudStorageAccount.Parse(ConfigurationManager.ConnectionStrings["storage"].ConnectionString);

            var client = account.CreateCloudQueueClient();

            var queue = client.GetQueueReference("tag-queue");

            await queue.CreateIfNotExistsAsync();

            var message = this.CreateIngestProcessActionMessage(action, source);

            await queue.AddMessageAsync(message);
        }

        private CloudQueueMessage CreateIngestProcessActionMessage(ProcessActionType action, TagSource source)
        {
            JsonConvert.DefaultSettings = () => { var s = new JsonSerializerSettings() { ContractResolver = new CamelCasePropertyNamesContractResolver(), NullValueHandling = NullValueHandling.Ignore }; s.Converters.Add(new StringEnumConverter() { CamelCaseText = true }); return s; };

            var message = new ProcessAction();
            message.Action = action;
            message.TagSourceUri = (source == null) ? null : source.Uri;

            var json = JsonConvert.SerializeObject(message);

            return new CloudQueueMessage(json);
        }
    }
}
