using System;

namespace EventProducerTest
{
    internal class Metrics
    {
        // Basic statistics
        public long TotalServiceOperations = 0;
        public long EventsPublished = 0;
        public long BatchesPublished = 0;
        public long TotalPublshedSizeBytes = 0;
        public double RunDurationMilliseconds = 0;

        // Exceptions
        public long CanceledSendExceptions = 0;
        public long SendExceptions = 0;
        public long TotalExceptions = 0;
        public long GeneralExceptions = 0;
        public long TimeoutExceptions = 0;
        public long CommunicationExceptions = 0;
        public long ServiceBusyExceptions = 0;
        public long ProducerRestarted = 0;
    }
}
