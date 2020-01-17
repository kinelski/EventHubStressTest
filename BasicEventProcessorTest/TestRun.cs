﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

        private ConcurrentDictionary<string, EventData> UnexpectedEvents { get; } = new ConcurrentDictionary<string, EventData>();

        private ConcurrentDictionary<string, byte> ProcessedEvents { get; } = new ConcurrentDictionary<string, byte>();

        private ConcurrentDictionary<string, long> LastReadPartitionSequence { get; } = new ConcurrentDictionary<string, long>();

        private TestConfiguration Configuration { get; }

        public TestRun(TestConfiguration configuration)
        {
            Configuration = configuration;
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            IsRunning = true;

            using var publishCancellationSource = new CancellationTokenSource();
            using var processorCancellationSource = new CancellationTokenSource();

            var publishingTask = default(Task);
            var processorTasks = default(IEnumerable<Task>);
            var runDuration = Stopwatch.StartNew();

            try
            {
                // Begin publishing events in the background.

                publishingTask = Task.Run(() => new Publisher(Configuration, Metrics, PublishedEvents, ErrorsObserved).Start(publishCancellationSource.Token));

                // Start processing.

                processorTasks = Enumerable
                    .Range(0, Configuration.ProcessorCount)
                    .Select(_ => Task.Run(() => new Processor(Configuration, Metrics, ErrorsObserved, ProcessEventHandler, ProcessErrorHandler).Start(processorCancellationSource.Token)))
                    .ToList();

                // Test for missing events and update metrics.

                var eventDueInterval = TimeSpan.FromMinutes(Configuration.EventReadLimitMinutes);

                while (!cancellationToken.IsCancellationRequested)
                {
                    Interlocked.Exchange(ref Metrics.RunDurationMilliseconds, runDuration.Elapsed.TotalMilliseconds);
                    ScanForUnreadEvents(PublishedEvents, UnexpectedEvents, ProcessedEvents, ErrorsObserved, eventDueInterval, Metrics);

                    await Task.Delay(TimeSpan.FromMinutes(5), cancellationToken).ConfigureAwait(false);
                }
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

            // The run is ending.  Clean up the outstanding background operations and
            // complete the necessary metrics tracking.

            try
            {
                publishCancellationSource.Cancel();
                await publishingTask.ConfigureAwait(false);

                // Wait a bit after publishing has completed before signaling for
                // processing to be canceled, to allow the recently published
                // events to be read.

                await Task.Delay(TimeSpan.FromMinutes(5)).ConfigureAwait(false);

                processorCancellationSource.Cancel();
                await Task.WhenAll(processorTasks).ConfigureAwait(false);

                // Wait a bit after processing has completed before and then perform
                // the last bit of bookkeeping, scanning for missing and unexpected events.

                await Task.Delay(TimeSpan.FromMinutes(2)).ConfigureAwait(false);
                ScanForUnreadEvents(PublishedEvents, UnexpectedEvents, ProcessedEvents, ErrorsObserved, TimeSpan.FromMinutes(Configuration.EventReadLimitMinutes), Metrics);

                foreach (var unexpectedEvent in UnexpectedEvents.Values)
                {
                    Interlocked.Increment(ref Metrics.UnknownEventsProcessed);
                    ErrorsObserved.Add(new EventHubsException(false, Configuration.EventHub, FormatUnexpectedEvent(unexpectedEvent, false), EventHubsException.FailureReason.GeneralError));
                }
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
                Interlocked.Increment(ref Metrics.TotalServiceOperations);

                // If there was no event then there is nothing to do.

                if (!args.HasEvent)
                {
                    return;
                }

                // Determine if the event has an identifier and has been tracked as published.

                var hasId = args.Data.Properties.TryGetValue(Property.Id, out var id);
                var eventId = (hasId) ? id.ToString() : null;
                var isTrackedEvent = PublishedEvents.TryRemove(eventId, out var publishedEvent);

                // If the event has an id that has been seen before, track it as a duplicate processing and
                // take no further action.

                if ((hasId) && (ProcessedEvents.ContainsKey(eventId)))
                {
                    Interlocked.Increment(ref Metrics.DuplicateEventsDiscarded);
                    return;
                }

                // Since this event wasn't a duplicate, consider it read.

                Interlocked.Increment(ref Metrics.EventsRead);

                // If there the event isn't a known and published event, then track it but take no
                // further action.

                if ((!hasId) || (!isTrackedEvent))
                {
                    // If there was an id, then the event wasn't tracked.  This is likely a race condition in tracking;
                    // allow for a small delay and then try to find the event again.

                    if (hasId)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(1)).ConfigureAwait(false);
                        isTrackedEvent = PublishedEvents.TryRemove(eventId, out publishedEvent);
                    }
                    else
                    {
                        // If there wasn't an id then consider it an unexpected event failure.

                        Interlocked.Increment(ref Metrics.UnknownEventsProcessed);
                        ErrorsObserved.Add(new EventHubsException(false, Configuration.EventHub, FormatUnexpectedEvent(args.Data, isTrackedEvent), EventHubsException.FailureReason.GeneralError));
                        return;
                    }

                    // If was an id, but the event wasn't tracked as published or processed, cache it as an
                    // unexpected event for later consideration.  If it cannot be cached, consider it a failure.

                    if ((!isTrackedEvent) && (!ProcessedEvents.ContainsKey(eventId)))
                    {
                        if (!UnexpectedEvents.TryAdd(eventId, args.Data))
                        {
                            Interlocked.Increment(ref Metrics.UnknownEventsProcessed);
                            ErrorsObserved.Add(new EventHubsException(false, Configuration.EventHub, FormatUnexpectedEvent(args.Data, isTrackedEvent), EventHubsException.FailureReason.GeneralError));
                        }

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

                // Mark the event as processed.

                Interlocked.Increment(ref Metrics.EventsProcessed);
                ProcessedEvents.TryAdd(eventId, 0);
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

                Interlocked.Increment(ref Metrics.TotalExceptions);
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

        private static void ScanForUnreadEvents(ConcurrentDictionary<string, EventData> publishedEvents,
                                                ConcurrentDictionary<string, EventData> unexpectedEvents,
                                                ConcurrentDictionary<string, byte> processedEvents,
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
                    // Check to see if the event was read and tracked as unexpected.  If so, perform basic validation
                    // and record it.

                    if (unexpectedEvents.TryRemove(publishedEvent.Key, out var readEvent))
                    {
                        Interlocked.Increment(ref metrics.EventsRead);

                        if (!CompareEventBodies(readEvent, publishedEvent.Value))
                        {
                            Interlocked.Increment(ref metrics.InvalidBodies);
                        }

                        if (!CompareEventProperties(readEvent, publishedEvent.Value))
                        {
                            Interlocked.Increment(ref metrics.InvalidProperties);
                        }

                        if ((!publishedEvent.Value.Properties.TryGetValue(Property.Partition, out var publishedPartition))
                            ||(!readEvent.Properties.TryGetValue(Property.Partition, out var readPartition))
                            || (readPartition.ToString() != publishedPartition.ToString()))
                        {
                            Interlocked.Increment(ref metrics.EventsFromWrongPartition);
                        }

                        Interlocked.Increment(ref metrics.EventsProcessed);
                        processedEvents.TryAdd(publishedEvent.Key, 0);
                    }
                    else if (!processedEvents.ContainsKey(publishedEvent.Key))
                    {
                        // The event wasn't read earlier and tracked as unexpected; it has not been seen.  Track it as missing.

                        Interlocked.Increment(ref metrics.EventsNotReceived);
                        errorsObserved.Add(new EventHubsException(false, string.Empty, FormatMissingEvent(publishedEvent.Value, now), EventHubsException.FailureReason.GeneralError));
                    }
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
                builder.AppendFormat("    Published: {0} ", ((DateTimeOffset)value).ToLocalTime().ToString("MM/dd/yyyy hh:mm:ss:tt"));
                builder.AppendLine();
            }

            builder.AppendFormat("    Classified Missing: {0} ", markedMissingTime.ToLocalTime().ToString("MM/dd/yyyy hh:mm:ss:tt"));
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
                builder.AppendFormat("    Published: {0} ", ((DateTimeOffset)value).ToLocalTime().ToString("MM/dd/yyyy hh:mm:ss:tt"));
                builder.AppendLine();
            }

            builder.AppendFormat("    Was in Published Events: {0} ", wasTrackedAsPublished);
            builder.AppendLine();

            return builder.ToString();
        }
    }
}
