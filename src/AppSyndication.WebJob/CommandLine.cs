using System;
using System.Collections.Generic;
using FireGiant.AppSyndication.Data;

namespace FireGiant.AppSyndication.WebJob
{
    public enum ProcessingCommand
    {
        Triggered,
        Continuous,
        Ingest,
        Store,
        Recalculate,
        Index,
    }

    public class CommandLine
    {
        public ProcessingCommand Command { get; private set; }

        public IEnumerable<string> Errors { get; private set; }

        public TagSource Source { get; private set; }

        public static CommandLine Parse(string[] args)
        {
            var commandLine = new CommandLine();

            var errors = new List<string>();

            if (args.Length == 0)
            {
                errors.Add("Must specify a command: triggered, continuous, ingest, store, recalculate, index");
            }
            else
            {
                var command = ProcessingCommand.Triggered;

                if (!Enum.TryParse<ProcessingCommand>(args[0], true, out command))
                {
                    errors.Add(String.Format("Unknown processing command: {0}. Supported commands are: triggered, continuous, ingest, store, recalculate, index", args[0]));
                }
                else
                {
                    commandLine.Command = command;

                    for (int i = 1; i < args.Length; ++i)
                    {
                        var arg = args[i];
                        if (arg.StartsWith("-") || arg.StartsWith("/"))
                        {
                            var param = arg.Substring(1);
                            switch (param.ToLowerInvariant())
                            {
                                default:
                                    errors.Add(String.Format("Unknown command-line paramter: {0}", arg));
                                    break;
                            }
                        }
                        else
                        {
                            TagSource source = null;

                            if (TagSource.TryParse(arg, out source))
                            {
                                commandLine.Source = source;
                            }
                            else
                            {
                                errors.Add(String.Format("Cannot parse '{0}' as tag source.", arg));
                            }
                        }
                    }
                }
            }

            if (commandLine.Source == null && commandLine.Command != ProcessingCommand.Continuous && commandLine.Command != ProcessingCommand.Triggered && commandLine.Command != ProcessingCommand.Recalculate && commandLine.Command != ProcessingCommand.Index)
            {
                errors.Add("A tag source must be specified.");
            }

            commandLine.Errors = errors;

            return commandLine;
        }
    }
}
