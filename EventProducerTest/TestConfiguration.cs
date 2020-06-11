using System;

namespace EventProducerTest
{
    internal class TestConfiguration
    {
        // Connection info

        public string EventHubsConnectionString;
        public string EventHub;

        // Publishing

        public int ProducerCount = 32;
        public int ConcurrentSends = 10;
        public int PublishBatchSize = 20;
        public int PublishingBodyMinBytes = 100;
        public int PublishingBodyRegularMaxBytes = 757760;
        public int LargeMessageRandomFactorPercent = 30;
        public TimeSpan SendTimeout = TimeSpan.FromMinutes(3);
        public TimeSpan? PublishingDelay = TimeSpan.FromMilliseconds(15);
    }
}
