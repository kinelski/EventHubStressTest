using System;

namespace EventProcessorTest
{
    internal class TestConfiguration
    {
        // Connection info

        public string EventHubsConnectionString;
        public string EventHub;
        public string StorageConnectionString;
        public string BlobContainer;

        // Publishing

        public int PublishBatchSize = 50;
        public int PublishingBodyMinBytes = 10;
        public int PublishingBodyRegularMaxBytes = 757760;
        public int LargeMessageRandomFactorPercent = 30;
        public TimeSpan SendTimeout = TimeSpan.FromMinutes(3);
        public TimeSpan? PublishingDelay = TimeSpan.FromMilliseconds(15);

        // Reading

        public int ProcessorCount = 2;
        public int EventReadLimitMinutes = 60;
        public TimeSpan ReadTimeout = TimeSpan.FromMinutes(1);
    }
}
