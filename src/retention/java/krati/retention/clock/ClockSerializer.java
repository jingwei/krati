package krati.retention.clock;

import krati.io.SerializationException;
import krati.io.Serializer;

/**
 * ClockSerializer
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 08/11, 2011 - Created
 */
public final class ClockSerializer implements Serializer<Clock> {
    
    @Override
    public Clock deserialize(byte[] bytes) throws SerializationException {
        return Clock.parseClock(bytes);
    }
    
    @Override
    public byte[] serialize(Clock clock) throws SerializationException {
        return clock == null ? null : clock.toByteArray();
    }
}
