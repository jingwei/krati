package krati.io.serializer;

import krati.io.SerializationException;
import krati.io.Serializer;

/**
 * ByteArraySerializer is a no-operation serializer for byte array.
 * 
 * @author jwu
 * @since 12/05, 2011
 */
public class ByteArraySerializer implements Serializer<byte[]>  {
    
    @Override
    public byte[] deserialize(byte[] bytes) throws SerializationException {
        return bytes;
    }
    
    @Override
    public byte[] serialize(byte[] object) throws SerializationException {
        return object;
    }
}
