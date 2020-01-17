using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace EventProcessorTest
{
    internal class Publisher
    {
        private static readonly ThreadLocal<Random> RandomNumberGenerator = new ThreadLocal<Random>(() => new Random(Interlocked.Increment(ref randomSeed)), false);

        private static int randomSeed = Environment.TickCount;

        private long currentSequence = 0;

        private Metrics Metrics { get; }

        private ConcurrentBag<Exception> ErrorsObserved { get; }

        private ConcurrentDictionary<string, EventData> PublishedEvents { get; }

        private TestConfiguration Configuration { get; }

        public Publisher(TestConfiguration configuration,
                         Metrics metrics,
                         ConcurrentDictionary<string, EventData> publishedEvents,
                         ConcurrentBag<Exception> errorsObserved)
        {
            Configuration = configuration;
            Metrics = metrics;
            PublishedEvents = publishedEvents;
            ErrorsObserved = errorsObserved;
        }

        public async Task Start(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var options = new EventHubProducerClientOptions
                    {
                        RetryOptions = new EventHubsRetryOptions
                        {
                           TryTimeout = Configuration.SendTimeout
                        }
                    };

                    var producer = new EventHubProducerClient(Configuration.EventHubsConnectionString, Configuration.EventHub, options);

                    await using (producer.ConfigureAwait(false))
                    {
                        while (!cancellationToken.IsCancellationRequested)
                        {
                            // Query partitions each iteration to allow any new partitions to be discovered. Dynamic upscaling of partitions
                            // is a near-term feature being added to the Event Hubs service.

                            var partitions = await producer.GetPartitionIdsAsync(cancellationToken).ConfigureAwait(false);
                            var selectedPartition = partitions[RandomNumberGenerator.Value.Next(0, partitions.Length)];

                            // Create the batch and generate a set of random events, keeping only those that were able to fit into the batch.
                            // Because there is a side-effect of TryAdd in the statement, ensure that ToList is called to materialize the set
                            // or the batch will be empty at send.

                            using var batch = await producer.CreateBatchAsync(new CreateBatchOptions { PartitionId = selectedPartition }).ConfigureAwait(false);

                            var batchEvents = Enumerable
                                .Range(0, Configuration.PublishBatchSize)
                                .Select(_ => GenerateEvent(batch.MaximumSizeInBytes, selectedPartition))
                                .Where(item => batch.TryAdd(item.Data))
                                .ToList();

                            // Publish the events and report them, capturing any failures specific to the send operation.

                            try
                            {
                                if (batch.Count > 0)
                                {
                                    await producer.SendAsync(batch, cancellationToken).ConfigureAwait(false);

                                    foreach (var batchEvent in batchEvents)
                                    {
                                        PublishedEvents.AddOrUpdate(batchEvent.Id, _ => batchEvent.Data, (k, v) => batchEvent.Data);
                                    }

                                    Interlocked.Add(ref Metrics.EventsPublished, batch.Count);
                                    Interlocked.Increment(ref Metrics.TotalServiceOperations);
                                }
                            }
                            catch (TaskCanceledException)
                            {
                            }
                            catch (EventHubsException ex)
                            {
                                Interlocked.Increment(ref Metrics.TotalExceptions);
                                Interlocked.Increment(ref Metrics.SendExceptions);
                                ex.TrackMetrics(Metrics);
                                ErrorsObserved.Add(ex);
                            }
                            catch (Exception ex)
                            {
                                Interlocked.Increment(ref Metrics.TotalExceptions);
                                Interlocked.Increment(ref Metrics.SendExceptions);
                                Interlocked.Increment(ref Metrics.GeneralExceptions);
                                ErrorsObserved.Add(ex);
                            }

                            // Honor any requested delays to throttle publishing so that it doesn't overwhelm slower consumers.

                            if ((Configuration.PublishingDelay.HasValue) && (Configuration.PublishingDelay.Value > TimeSpan.Zero))
                            {
                                await Task.Delay(Configuration.PublishingDelay.Value).ConfigureAwait(false);
                            }
                        }
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
                    Interlocked.Increment(ref Metrics.ProducerRestarted);
                    ErrorsObserved.Add(ex);
                }
            }
        }

        private (string Id, EventData Data) GenerateEvent(long maxMessageSize,
                                                          string partition)
        {
            // Allow a chance to generate a large size event, otherwise, randomly
            // size within the normal range.

            long bodySize;

            if (RandomNumberGenerator.Value.NextDouble() < Configuration.LargeMessageRandomFactor)
            {
                bodySize = (Configuration.PublishingBodyMinBytes + (long)(RandomNumberGenerator.Value.NextDouble() * (maxMessageSize - Configuration.PublishingBodyMinBytes)));
            }
            else
            {
                bodySize = RandomNumberGenerator.Value.Next(Configuration.PublishingBodyMinBytes, Configuration.PublishingBodyRegularMaxBytes);
            }

            var id = Guid.NewGuid().ToString();
            var body = new byte[bodySize];
            RandomNumberGenerator.Value.NextBytes(body);

            var eventData = new EventData(body);
            eventData.Properties[Property.Id] = id;
            eventData.Properties[Property.Partition] = partition;
            eventData.Properties[Property.Sequence] = Interlocked.Increment(ref currentSequence);
            eventData.Properties[Property.PublishDate] = DateTimeOffset.UtcNow;

            return (id, eventData);
        }
    }
}
