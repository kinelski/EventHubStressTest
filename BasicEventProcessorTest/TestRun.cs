using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Processor;

namespace EventProcessorTest
{
    internal class TestRun
    {
        public string Id { get; } = Guid.NewGuid().ToString();

        public bool IsRunning { get; private set; } = false;

        public Metrics Metrics { get; } = new Metrics();

        public ConcurrentBag<Exception> ErrorsObserved { get; } = new ConcurrentBag<Exception>();

        private ConcurrentDictionary<string, EventData> PublishedEvents { get; } = new ConcurrentDictionary<string, EventData>();

        private ConcurrentDictionary<string, long> LastReadPartitionSequence { get; } = new ConcurrentDictionary<string, long>();

        private TestConfiguration Configuration { get; }

        public TestRun(TestConfiguration configuration)
        {
            Configuration = configuration;
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            IsRunning = true;

            var runDuration = Stopwatch.StartNew();

            try
            {
                // Begin publishing events in the background.

                var publishingTask = Task.Run(() => new Publisher(Configuration, Metrics, PublishedEvents, ErrorsObserved).Start(cancellationToken));

                // Start processing.

                var processorTasks = Enumerable
                    .Range(0, Configuration.ProcessorCount)
                    .Select(_ => Task.Run(() => new Processor(Configuration, Metrics, PublishedEvents, ErrorsObserved, ProcessEventHandler, ProcessErrorHandler).Start(cancellationToken)))
                    .ToList();

                // Test for missing events and update metrics.

                var eventDueInterval = TimeSpan.FromMinutes(Configuration.EventReadLimitMinutes);

                while (!cancellationToken.IsCancellationRequested)
                {
                    Interlocked.Exchange(ref Metrics.RunDurationMilliseconds, runDuration.Elapsed.TotalMilliseconds);
                    TrackUnreadEvents(PublishedEvents, ErrorsObserved, eventDueInterval, Metrics);

                    await Task.Delay(TimeSpan.FromMinutes(5), cancellationToken).ConfigureAwait(false);
                }

                // The run is ending.  Clean up the outstanding background operations.  Wait
                // for a period of time between the

                await publishingTask.ConfigureAwait(false);
                await Task.WhenAll(processorTasks).ConfigureAwait(false);
            }
            catch (TaskCanceledException)
            {
                // No action needed.
            }
            catch (Exception ex) when
                (ex is OutOfMemoryException
                || ex is StackOverflowException
                || ex is ThreadAbortException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref Metrics.TotalExceptions);
                Interlocked.Increment(ref Metrics.GeneralExceptions);
                ErrorsObserved.Add(ex);
            }
            finally
            {
                runDuration.Stop();
                IsRunning = false;
            }
        }

        private async Task ProcessEventHandler(ProcessEventArgs args)
        {
            try
            {
                // If there was no event then there is nothing to do.

                if (!args.HasEvent)
                {
                    return;
                }

                Interlocked.Increment(ref Metrics.TotalServiceOperations);
                Interlocked.Increment(ref Metrics.EventsRead);

                // If there the event isn't a known and published event, then track it but take no
                // further action.

                var hasId = args.Data.Properties.TryGetValue(Property.Id, out var eventId);
                var isTrackedEvent = PublishedEvents.TryRemove(eventId.ToString(), out var publishedEvent);

                if ((!hasId) || (!isTrackedEvent))
                {
                    // If there was an id, then the event wasn't tracked.  This is likely a race condition in tracking;
                    // allow for a small delay and then try to find the event again.

                    if (hasId)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                        isTrackedEvent = PublishedEvents.TryRemove(eventId.ToString(), out publishedEvent);
                    }

                    // If there wasn't an id or the event wasn't found in the published set, then consider it
                    // an unexpected event.

                    if ((!hasId) || (!isTrackedEvent))
                    {
                        Interlocked.Increment(ref Metrics.UnknownEventsProcessed);
                        ErrorsObserved.Add(new EventHubsException(false, Configuration.EventHub, FormatUnexpectedEvent(args.Data, isTrackedEvent), EventHubsException.FailureReason.GeneralError));
                        return;
                    }
                }

                // Validate the event against expectations.

                if (!CompareEventBodies(args.Data, publishedEvent))
                {
                    Interlocked.Increment(ref Metrics.InvalidBodies);
                }

                if (!CompareEventProperties(args.Data, publishedEvent))
                {
                    Interlocked.Increment(ref Metrics.InvalidProperties);
                }

                // Validate that the intended partition that was sent as a property matches the
                // partition that the handler was triggered for.

                if ((!publishedEvent.Properties.TryGetValue(Property.Partition, out var publishedPartition))
                    || (args.Partition.PartitionId != publishedPartition.ToString()))
                {
                    Interlocked.Increment(ref Metrics.EventsFromWrongPartition);
                }

                // Validate that the sequence number that was sent as a property is greater than the last read
                // sequence number for the partition; if there hasn't been an event for the partition yet, then
                // there is no validation possible.
                //
                // Track the sequence number for future comparisons.

                var currentSequence = default(object);

                if ((LastReadPartitionSequence.TryGetValue(args.Partition.PartitionId, out var lastReadSequence))
                    && ((!publishedEvent.Properties.TryGetValue(Property.Sequence, out currentSequence)) || (lastReadSequence >= (long)currentSequence)))
                {
                    Interlocked.Increment(ref Metrics.EventsOutOfOrder);
                }

                var trackSequence = (currentSequence == default) ? -1 : (long)currentSequence;
                LastReadPartitionSequence.AddOrUpdate(args.Partition.PartitionId, _ => trackSequence, (part, seq) => Math.Max(seq, trackSequence));

                // Create a checkpoint every 100 events for the partition, just to follow expected patterns.

                if (trackSequence % 100 == 0)
                {
                    await args.UpdateCheckpointAsync(args.CancellationToken).ConfigureAwait(false);
                }
            }
            catch (EventHubsException ex)
            {
                Interlocked.Increment(ref Metrics.TotalExceptions);
                Interlocked.Increment(ref Metrics.ProcessingExceptions);
                ex.TrackMetrics(Metrics);
                ErrorsObserved.Add(ex);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref Metrics.TotalExceptions);
                Interlocked.Increment(ref Metrics.ProcessingExceptions);
                Interlocked.Increment(ref Metrics.GeneralExceptions);
                ErrorsObserved.Add(ex);
            }
        }

        private Task ProcessErrorHandler(ProcessErrorEventArgs args)
        {
            try
            {
                var eventHubsException = (args.Exception as EventHubsException);
                eventHubsException?.TrackMetrics(Metrics);

                if (eventHubsException == null)
                {
                    Interlocked.Increment(ref Metrics.GeneralExceptions);
                }

                Interlocked.Increment(ref Metrics.ProcessingExceptions);
                ErrorsObserved.Add(args.Exception);
            }
            catch (EventHubsException ex)
            {
                Interlocked.Increment(ref Metrics.TotalExceptions);
                ex.TrackMetrics(Metrics);
                ErrorsObserved.Add(ex);
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref Metrics.TotalExceptions);
                Interlocked.Increment(ref Metrics.GeneralExceptions);
                ErrorsObserved.Add(ex);
            }

            return Task.CompletedTask;
        }

        private static bool CompareEventBodies(EventData first,
                                               EventData second)
        {
            if (object.ReferenceEquals(first, second))
            {
                return true;
            }

            if ((first == null) || (second == null))
            {
                return false;
            }

            var firstBody = first.Body.ToArray();
            var secondBody = second.Body.ToArray();

            if ((firstBody == null) && (secondBody == null))
            {
                return true;
            }

            if ((firstBody == null) || (secondBody == null))
            {
                return false;
            }

            if (firstBody.Length != secondBody.Length)
            {
                return false;
            }

            for (var index = 0; index < firstBody.Length; ++index)
            {
                if (firstBody[index] != secondBody[index])
                {
                    return false;

                }
            }

            return true;
        }

        private static bool CompareEventProperties(EventData first,
                                                   EventData second)
        {
            if (object.ReferenceEquals(first, second))
            {
                return true;
            }

            if ((first == null) || (second == null))
            {
                return false;
            }

            var firstProps = first.Properties;
            var secondProps = second.Properties;

            if (object.ReferenceEquals(firstProps, secondProps))
            {
                return true;
            }

            if ((firstProps == null) || (secondProps == null))
            {
                return false;
            }

            if (firstProps.Count != secondProps.Count)
            {
                return false;
            }

            return firstProps.OrderBy(kvp => kvp.Key).SequenceEqual(secondProps.OrderBy(kvp => kvp.Key));
        }

        private static void TrackUnreadEvents(ConcurrentDictionary<string, EventData> publishedEvents,
                                              ConcurrentBag<Exception> errorsObserved,
                                              TimeSpan eventDueInterval,
                                              Metrics metrics)
        {
            // An event is considered missing if it was published longer ago than the due time and
            // still exists in the set of published events.

            object publishDate;

            var now = DateTimeOffset.UtcNow;

            foreach (var publishedEvent in publishedEvents.ToList())
            {
                if ((publishedEvent.Value.Properties.TryGetValue(Property.PublishDate, out publishDate))
                    && (((DateTimeOffset)publishDate).Add(eventDueInterval) < now)
                    && (publishedEvents.TryRemove(publishedEvent.Key, out _)))
                {
                    Interlocked.Increment(ref metrics.EventsNotReceived);
                    errorsObserved.Add(new EventHubsException(false, string.Empty, FormatMissingEvent(publishedEvent.Value, now), EventHubsException.FailureReason.GeneralError));
                }
            }
        }

        private static string FormatMissingEvent(EventData eventData,
                                                 DateTimeOffset markedMissingTime)
        {
            var builder = new StringBuilder();
            builder.AppendLine("Event Not Received:");

            object value;

            if (eventData.Properties.TryGetValue(Property.Id, out value))
            {
                builder.AppendFormat("    Event Id: {0} ", value);
                builder.AppendLine();
            }

            if (eventData.Properties.TryGetValue(Property.Partition, out value))
            {
                builder.AppendFormat("    Sent To Partition: {0} ", value);
                builder.AppendLine();
            }

            if (eventData.Properties.TryGetValue(Property.Sequence, out value))
            {
                builder.AppendFormat("    Artificial Sequence: {0} ", value);
                builder.AppendLine();
            }

            if (eventData.Properties.TryGetValue(Property.PublishDate, out value))
            {
                builder.AppendFormat("    Published: {0} ", ((DateTimeOffset)value).ToLocalTime().ToString("mm/dd/yyyy hh:mm:ss:tt"));
                builder.AppendLine();
            }

            builder.AppendFormat("    Classified Missing: {0} ", markedMissingTime.ToLocalTime().ToString("mm/dd/yyyy hh:mm:ss:tt"));
            builder.AppendLine();

            return builder.ToString();
        }

        private static string FormatUnexpectedEvent(EventData eventData,
                                                    bool wasTrackedAsPublished)
        {
            var builder = new StringBuilder();
            builder.AppendLine("Unexpected Event:");

            object value;

            if (eventData.Properties.TryGetValue(Property.Id, out value))
            {
                builder.AppendFormat("    Event Id: {0} ", value);
                builder.AppendLine();
            }

            if (eventData.Properties.TryGetValue(Property.Partition, out value))
            {
                builder.AppendFormat("    Sent To Partition: {0} ", value);
                builder.AppendLine();
            }

            if (eventData.Properties.TryGetValue(Property.Sequence, out value))
            {
                builder.AppendFormat("    Artificial Sequence: {0} ", value);
                builder.AppendLine();
            }

            if (eventData.Properties.TryGetValue(Property.PublishDate, out value))
            {
                builder.AppendFormat("    Published: {0} ", ((DateTimeOffset)value).ToLocalTime().ToString("mm/dd/yyyy hh:mm:ss:tt"));
                builder.AppendLine();
            }

            builder.AppendFormat("    Was in Published Events: {0} ", wasTrackedAsPublished);
            builder.AppendLine();

            return builder.ToString();
        }
    }
}
