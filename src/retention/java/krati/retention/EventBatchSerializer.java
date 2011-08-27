package krati.retention;

import krati.io.SerializationException;
import krati.io.Serializer;

/**
 * EventBatchSerializer
 * 
 * @version 0.4.2
 * @author jwu
 * 
 * <p>
 * 07/31, 2011 - Created
 */
public interface EventBatchSerializer<T> extends Serializer<EventBatch<T>> {
    
    public EventBatchHeader deserializeHeader(byte[] bytes) throws SerializationException;
    
    public byte[] serializeHeader(EventBatchHeader header) throws SerializationException;
}
