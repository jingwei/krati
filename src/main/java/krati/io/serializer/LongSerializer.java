package krati.io.serializer;

import java.nio.ByteOrder;

import krati.io.SerializationException;
import krati.io.Serializer;
import krati.util.Numbers;

/**
 * LongSerializer
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class LongSerializer implements Serializer<Long> {
    private final ByteOrder _byteOrder;
    
    /**
     * Creates a new long Serializer using the BIG_ENDIAN byte order.
     */
    public LongSerializer() {
        this._byteOrder = ByteOrder.BIG_ENDIAN;
    }
    
    /**
     * Creates a new long Serializer using the specified byte order.
     */
    public LongSerializer(ByteOrder byteOrder) {
        this._byteOrder = (byteOrder == null) ? ByteOrder.BIG_ENDIAN : byteOrder;
    }
    
    @Override
    public Long deserialize(byte[] bytes) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.longValueBE(bytes) : Numbers.longValueLE(bytes);
    }
    
    @Override
    public byte[] serialize(Long value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.longBytesBE(value) : Numbers.longBytesLE(value);
    }
    
    public long longValue(byte[] bytes) {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.longValueBE(bytes) : Numbers.longValueLE(bytes);
    }
    
    public byte[] longBytes(long value) {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.longBytesBE(value) : Numbers.longBytesLE(value);
    }
}
