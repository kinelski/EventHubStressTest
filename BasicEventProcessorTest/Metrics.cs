using System;

namespace EventProcessorTest
{
    internal class Metrics
    {
        // Basic statistics
        public long TotalServiceOperations = 0;
        public long EventsPublished = 0;
        public long EventsRead = 0;
        public long EventsProcessed = 0;
        public double RunDurationMilliseconds = 0;

        // Event validation issues
        public long InvalidBodies = 0;
        public long InvalidProperties = 0;
        public long EventsNotReceived = 0;
        public long EventsOutOfOrder = 0;
        public long EventsFromWrongPartition = 0;
        public long UnknownEventsProcessed = 0;
        public long DuplicateEventsProcessed = 0;

        // Exceptions
        public long SendExceptions = 0;
        public long ProcessingExceptions = 0;
        public long TotalExceptions = 0;
        public long GeneralExceptions = 0;
        public long TimeoutExceptions = 0;
        public long CommunicationExceptions = 0;
        public long ServiceBusyExceptions = 0;
        public long ProcessorRestarted = 0;
        public long ProducerRestarted = 0;
    }
}
