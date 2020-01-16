using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventProcessorTest
{
    public static class Program
    {
        private static readonly TimeSpan DefaultProcessReportInterval = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan DefaultRunDuration = TimeSpan.FromHours(72);
        private static readonly string DefaultErrorLogPath = Path.Combine(Environment.CurrentDirectory, $"processor-errors-{ DateTime.Now.ToString("yyyy-MM-dd_hh-mm-ss") }.log");

        public static async Task Main(string[] args)
        {
            var runArgs = ParseAndPromptForArguments(args);
            var runDuration = DefaultRunDuration;
            var errorLogPath = DefaultErrorLogPath;

            // If not provided or malformed, use the default.

            if ((!string.IsNullOrEmpty(runArgs.RunDurationHours)) && (int.TryParse(runArgs.RunDurationHours, out var hours)))
            {
                runDuration = TimeSpan.FromHours(hours);
            }

            if (!string.IsNullOrEmpty(runArgs.LogPath))
            {
                errorLogPath = runArgs.LogPath;
            }

            using var cancellationSource = new CancellationTokenSource();
            using var errorWriter = new StreamWriter(File.Open(errorLogPath, FileMode.Create, FileAccess.Write, FileShare.Read));
            using var metricsWriter = Console.Out;

            try
            {
                var message = $"{ Environment.NewLine }{ Environment.NewLine }=============================={ Environment.NewLine }  Run Starting{ Environment.NewLine }=============================={ Environment.NewLine }";
                metricsWriter.WriteLine(message);
                errorWriter.WriteLine(message);

                cancellationSource.CancelAfter(runDuration);

                var testRun = new TestRun(new TestConfiguration
                {
                    EventHubsConnectionString = runArgs.EventHubsConnectionString,
                    EventHub = runArgs.EventHub,
                    StorageConnectionString = runArgs.StorageConnectionString,
                    BlobContainer = runArgs.BlobContainer
                });

                var testRunTask = testRun.Start(cancellationSource.Token);

                while (!cancellationSource.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(DefaultProcessReportInterval, cancellationSource.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        message = $"{ Environment.NewLine }{ Environment.NewLine }------------------------------------------------------------{ Environment.NewLine }  The run is ending.  Waiting for clean-up and final reporting...{ Environment.NewLine }------------------------------------------------------------";
                        metricsWriter.WriteLine(message);
                        errorWriter.WriteLine(message);
                    }

                    await Task.WhenAll
                    (
                        ReportMetricsAsync(metricsWriter, testRun.Metrics, runDuration),
                        ReportErrorsAsync(errorWriter, testRun.ErrorsObserved)
                    );
                }

                // Allow the run to complete and then perform a final pas on reporting
                // to ensure that any straggling operations are captures.

                await testRunTask;
                Interlocked.Exchange(ref testRun.Metrics.RunDurationMilliseconds, runDuration.TotalMilliseconds);

                await Task.WhenAll
                (
                    ReportMetricsAsync(metricsWriter, testRun.Metrics, runDuration),
                    ReportErrorsAsync(errorWriter, testRun.ErrorsObserved)
                );

                message = $"{ Environment.NewLine }{ Environment.NewLine }=============================={ Environment.NewLine }  Run Complete{ Environment.NewLine }==============================";
                metricsWriter.WriteLine(message);
                errorWriter.WriteLine(message);
            }
            catch (Exception ex) when
                (ex is OutOfMemoryException
                || ex is StackOverflowException
                || ex is ThreadAbortException)
            {
                Environment.FailFast(ex.Message);
            }
            catch (Exception ex)
            {
                var message = $"{ Environment.NewLine }{ Environment.NewLine }=============================={ Environment.NewLine }  Error in the main loop.  Run aborting.  Message: [{ ex.Message }]{ Environment.NewLine }==============================";
                metricsWriter.WriteLine(message);
                errorWriter.WriteLine(message);
            }
            finally
            {
                errorWriter.Close();
            }
        }

        private static Task ReportMetricsAsync(TextWriter writer,
                                               Metrics metrics,
                                               TimeSpan runDuration)
        {
            var message = new StringBuilder();
            var metric = default(long);

            // Run time

            var runDurationMilliseconds = Interlocked.CompareExchange(ref metrics.RunDurationMilliseconds, 0.0, 0.0);
            var currentDuration = TimeSpan.FromMilliseconds(runDurationMilliseconds > 0.0 ? runDurationMilliseconds : 1);

            message.AppendLine("Run Metrics");
            message.AppendLine("=========================");
            message.AppendLine($"\tRun Duration:\t\t\t{ runDuration.ToString(@"dd\.hh\:mm\:ss") }");
            message.AppendLine($"\tElapsed:\t\t\t{ currentDuration.ToString(@"dd\.hh\:mm\:ss") } ({ (currentDuration / runDuration).ToString("P", CultureInfo.InvariantCulture) })");
            message.AppendLine();

            // Publish and read pairing

            message.AppendLine("Publishing and Receiving");
            message.AppendLine("=========================");

            var serviceOps = (double)Interlocked.Read(ref metrics.TotalServiceOperations);
            message.AppendLine($"\tService Operations:\t\t{ serviceOps.ToString("n0") }");
            serviceOps = (serviceOps > 0) ? serviceOps : 0.001;

            var published = (double)Interlocked.Read(ref metrics.EventsPublished);
            message.AppendLine($"\tEvents Published:\t\t{ published.ToString("n0") }");
            published = (published > 0) ? published : 0.001;

            var read = (double)Interlocked.Read(ref metrics.EventsRead);
            message.AppendLine($"\tEvents Read:\t\t\t{ read.ToString("n0") } ({ (read / published).ToString("P", CultureInfo.InvariantCulture) })");
            read = (read > 0) ? read : 0.001;

            metric = Interlocked.Read(ref metrics.EventsProcessed);
            message.AppendLine($"\tEvents Processed:\t\t{ read.ToString("n0") } ({ (metric / published).ToString("P", CultureInfo.InvariantCulture) })");

            message.AppendLine();

            // Validation issues

            message.AppendLine("Event Validation");
            message.AppendLine("=========================");

            metric = Interlocked.Read(ref metrics.EventsNotReceived);
            message.AppendLine($"\tEvents Not Received:\t\t{ metric.ToString("n0") } ({ (metric / published).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.UnknownEventsProcessed);
            message.AppendLine($"\tUnexpected Events Received:\t{ metric.ToString("n0") } ({ (metric / read).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.DuplicateEventsProcessed);
            message.AppendLine($"\tDuplicate Events Processed:\t{ metric.ToString("n0") } ({ (metric / read).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.InvalidBodies);
            message.AppendLine($"\tEvents with Invalid Bodies:\t{ metric.ToString("n0") } ({ (metric / read).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.InvalidProperties);
            message.AppendLine($"\tEvents with Invalid Properties:\t{ metric.ToString("n0") } ({ (metric / read).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.EventsFromWrongPartition);
            message.AppendLine($"\tEvents From a Wrong Partition:\t{ metric.ToString("n0") } ({ (metric / read).ToString("P", CultureInfo.InvariantCulture) })");

            message.AppendLine();

            // Client health

            message.AppendLine("Client Health");
            message.AppendLine("=========================");

            metric = Interlocked.Read(ref metrics.ProcessorRestarted);
            message.AppendLine($"\tProcessor Restarts:\t\t{ metric }");

            metric = Interlocked.Read(ref metrics.ProducerRestarted);
            message.AppendLine($"\tProducer Restarts:\t\t{ metric }");

            message.AppendLine();

            // Exceptions

            message.AppendLine("Exception Breakdown");
            message.AppendLine("=========================");

            var totalExceptions = (double)Interlocked.Read(ref metrics.TotalExceptions);
            message.AppendLine($"\tExceptions for All Operations:\t{ totalExceptions.ToString("n0") } ({ (totalExceptions / serviceOps).ToString("P", CultureInfo.InvariantCulture) })");
            totalExceptions = (totalExceptions > 0) ? totalExceptions : 0.001;

            metric = Interlocked.Read(ref metrics.SendExceptions);
            message.AppendLine($"\tException During Send:\t\t{ metric.ToString("n0") } ({ (metric / totalExceptions).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.ProcessingExceptions);
            message.AppendLine($"\tException During Processing:\t{ metric.ToString("n0") } ({ (metric / totalExceptions).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.GeneralExceptions);
            message.AppendLine($"\tGeneral Exceptions:\t\t{ metric.ToString("n0") } ({ (metric / totalExceptions).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.TimeoutExceptions);
            message.AppendLine($"\tTimeout Exceptions:\t\t{ metric.ToString("n0") } ({ (metric / totalExceptions).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.CommunicationExceptions);
            message.AppendLine($"\tCommunication Exceptions:\t{ metric.ToString("n0") } ({ (metric / totalExceptions).ToString("P", CultureInfo.InvariantCulture) })");

            metric = Interlocked.Read(ref metrics.ServiceBusyExceptions);
            message.AppendLine($"\tService Busy Exceptions:\t{ metric.ToString("n0") } ({ (metric / totalExceptions).ToString("P", CultureInfo.InvariantCulture) })");

            // Spacing

            message.AppendLine();
            message.AppendLine();
            message.AppendLine();
            message.AppendLine();

            return writer.WriteLineAsync(message.ToString());
        }

         private static async Task ReportErrorsAsync(TextWriter writer,
                                                     ConcurrentBag<Exception> exceptions)
        {
            Exception currentException;

            while (exceptions.TryTake(out currentException))
            {
                await writer.WriteLineAsync
                (
                    $"[ { currentException.GetType().Name } ]{Environment.NewLine}{ currentException.Message ?? "No message available" }{ Environment.NewLine }{ currentException.StackTrace ?? "No stack trace available" }{ Environment.NewLine }"
                );
            }

            writer.Flush();
        }

        private static CommandLineArguments ParseAndPromptForArguments(string[] commandLineArgs)
        {
            var parsedArgs = ParseArguments(commandLineArgs);

            // Prompt for the Event Hubs connection string, if it wasn't passed.

            while (string.IsNullOrEmpty(parsedArgs.EventHubsConnectionString))
            {
                Console.Write("Please provide the connection string for the Event Hubs namespace that you'd like to use and then press Enter: ");
                parsedArgs.EventHubsConnectionString = Console.ReadLine().Trim();
                Console.WriteLine();
            }

            // Prompt for the Event Hub name, if it wasn't passed.

            while (string.IsNullOrEmpty(parsedArgs.EventHub))
            {
                Console.Write("Please provide the name of the Event Hub that you'd like to use and then press Enter: ");
                parsedArgs.EventHub = Console.ReadLine().Trim();
                Console.WriteLine();
            }

            // Prompt for the storage connection string, if it wasn't passed.

            while (string.IsNullOrEmpty(parsedArgs.StorageConnectionString))
            {
                Console.Write("Please provide the connection string for the Azure storage account that you'd like to use and then press Enter: ");
                parsedArgs.StorageConnectionString = Console.ReadLine().Trim();
                Console.WriteLine();
            }

            // Prompt for the blob container name, if it wasn't passed.

            while (string.IsNullOrEmpty(parsedArgs.BlobContainer))
            {
                Console.Write("Please provide the name of the blob container that you'd like to use and then press Enter: ");
                parsedArgs.BlobContainer = Console.ReadLine().Trim();
                Console.WriteLine();
            }

            return parsedArgs;
        }

        private static CommandLineArguments ParseArguments(string[] args)
        {
            // If at least four arguments were passed with no argument designator, then assume they're values and
            // accept them positionally.

            if ((args.Length >= 4)
                && (!args[0].StartsWith(CommandLineArguments.ArgumentPrefix))
                && (!args[1].StartsWith(CommandLineArguments.ArgumentPrefix))
                && (!args[2].StartsWith(CommandLineArguments.ArgumentPrefix))
                && (!args[3].StartsWith(CommandLineArguments.ArgumentPrefix)))
            {
                var parsed = new CommandLineArguments
                {
                    EventHubsConnectionString = args[0],
                    EventHub = args[1],
                    StorageConnectionString = args[2],
                    BlobContainer = args[3]
                };

                if ((args.Length >= 5) && (!args[4].StartsWith(CommandLineArguments.ArgumentPrefix)))
                {
                    parsed.RunDurationHours = args[4];
                }

                if ((args.Length >= 6) && (!args[5].StartsWith(CommandLineArguments.ArgumentPrefix)))
                {
                    parsed.LogPath = args[4];
                }

                return parsed;
            }

            var parsedArgs = new CommandLineArguments();

            // Enumerate the arguments that were passed, stopping one before the
            // end, since we're scanning forward by an item to retrieve values;  if a
            // command was passed in the last position, there was no accompanying value,
            // so it isn't useful.

            for (var index = 0; index < args.Length - 1; ++index)
            {
                // Remove any excess spaces to comparison purposes.

                args[index] = args[index].Trim();

                // Since we're evaluating the next token in sequence as a value in the
                // checks that follow, if it is an argument, we'll skip to the next iteration.

                if (args[index + 1].StartsWith(CommandLineArguments.ArgumentPrefix))
                {
                    continue;
                }

                // If the current token is one of our known arguments, capture the next token in sequence as it's
                // value, since we've already ruled out that it is another argument name.

                if (args[index].Equals($"{ CommandLineArguments.ArgumentPrefix }{ nameof(CommandLineArguments.EventHubsConnectionString) }", StringComparison.OrdinalIgnoreCase))
                {
                    parsedArgs.EventHubsConnectionString = args[index + 1].Trim();
                }
                else if (args[index].Equals($"{ CommandLineArguments.ArgumentPrefix }{ nameof(CommandLineArguments.EventHub) }", StringComparison.OrdinalIgnoreCase))
                {
                    parsedArgs.EventHub = args[index + 1].Trim();
                }
                else if (args[index].Equals($"{ CommandLineArguments.ArgumentPrefix }{ nameof(CommandLineArguments.StorageConnectionString) }", StringComparison.OrdinalIgnoreCase))
                {
                    parsedArgs.StorageConnectionString = args[index + 1].Trim();
                }
                else if (args[index].Equals($"{ CommandLineArguments.ArgumentPrefix }{ nameof(CommandLineArguments.BlobContainer) }", StringComparison.OrdinalIgnoreCase))
                {
                    parsedArgs.BlobContainer = args[index + 1].Trim();
                }
                else if (args[index].Equals($"{ CommandLineArguments.ArgumentPrefix }{ nameof(CommandLineArguments.RunDurationHours) }", StringComparison.OrdinalIgnoreCase))
                {
                    parsedArgs.RunDurationHours = args[index + 1].Trim();
                }
                else if (args[index].Equals($"{ CommandLineArguments.ArgumentPrefix }{ nameof(CommandLineArguments.LogPath) }", StringComparison.OrdinalIgnoreCase))
                {
                    parsedArgs.LogPath = args[index + 1].Trim();
                }
            }

            return parsedArgs;
        }

        private class CommandLineArguments
        {
            public const string ArgumentPrefix = "--";
            public string EventHubsConnectionString;
            public string EventHub;
            public string StorageConnectionString;
            public string BlobContainer;
            public string RunDurationHours;
            public string LogPath;
        }
    }
}
