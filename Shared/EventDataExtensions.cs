using System;
using System.Linq;

namespace Azure.Messaging.EventHubs.Tests
{
    internal static class EventDataExtensions
    {
        public static bool IsEquivalentTo(this EventData instance,
                                          EventData other,
                                          bool considerSystemProperties = false)
        {
            // If the events are the same instance, they're equal.  This should only happen
            // if both are null or they are the exact same instance.

            if (Object.ReferenceEquals(instance, other))
            {
                return true;
            }

            // If one or the other is null, then they cannot be equal, since we know that
            // they are not both null.

            if ((instance == null) || (other == null))
            {
                return false;
            }

            // If the contents of each body is not equal, the events are not
            // equal.

            var instanceBody = instance.Body.ToArray();
            var otherBody = other.Body.ToArray();

            if (instanceBody.Length != otherBody.Length)
            {
                return false;
            }

            if (!Enumerable.SequenceEqual(instanceBody, otherBody))
            {
                return false;
            }

            // Verify the system properties are equivalent, unless they're the same reference.

            if ((considerSystemProperties) && (!Object.ReferenceEquals(instance.SystemProperties, other.SystemProperties)))
            {
                if ((instance.SystemProperties == null) || (other.SystemProperties == null))
                {
                    return false;
                }

                if (instance.SystemProperties.Count != other.SystemProperties.Count)
                {
                    return false;
                }

                if ((instance.Offset != other.Offset)
                    || (instance.EnqueuedTime != other.EnqueuedTime)
                    || (instance.PartitionKey != other.PartitionKey)
                    || (instance.SequenceNumber != other.SequenceNumber))
                {
                    return false;
                }

                if (!instance.SystemProperties.OrderBy(kvp => kvp.Key).SequenceEqual(other.SystemProperties.OrderBy(kvp => kvp.Key)))
                {
                    return false;
                }
            }

            // Since we know that the event bodies and system properties are equal, if the property sets are the
            // same instance, then we know that the events are equal.  This should only happen if both are null.

            if (Object.ReferenceEquals(instance.Properties, other.Properties))
            {
                return true;
            }

            // If either property is null, then the events are not equal, since we know that they are
            // not both null.

            if ((instance.Properties == null) || (other.Properties == null))
            {
                return false;
            }

            // The only meaningful comparison left is to ensure that the property sets are equivalent,
            // the outcome of this check is the final word on equality.

            if (instance.Properties.Count != other.Properties.Count)
            {
                return false;
            }

            return instance.Properties.OrderBy(kvp => kvp.Key).SequenceEqual(other.Properties.OrderBy(kvp => kvp.Key));
        }
    }
}