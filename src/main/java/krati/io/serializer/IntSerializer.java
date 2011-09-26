package krati.io.serializer;

import java.nio.ByteOrder;

import krati.io.SerializationException;
import krati.io.Serializer;
import krati.util.Numbers;

/**
 * IntSerializer
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class IntSerializer implements Serializer<Integer> {
    private final ByteOrder _byteOrder;
    
    /**
     * Creates a new int Serializer using the BIG_ENDIAN byte order.
     */
    public IntSerializer() {
        this._byteOrder = ByteOrder.BIG_ENDIAN;
    }
    
    /**
     * Creates a new int Serializer using the specified byte order.
     */
    public IntSerializer(ByteOrder byteOrder) {
        this._byteOrder = (byteOrder == null) ? ByteOrder.BIG_ENDIAN : byteOrder;
    }
    
    @Override
    public Integer deserialize(byte[] bytes) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.intValueBE(bytes) : Numbers.intValueLE(bytes);
    }
    
    @Override
    public byte[] serialize(Integer value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.intBytesBE(value) : Numbers.intBytesLE(value);
    }
    
    public int intValue(byte[] bytes) {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.intValueBE(bytes) : Numbers.intValueLE(bytes);
    }
    
    public byte[] intBytes(int value) throws SerializationException {
        return (_byteOrder == ByteOrder.BIG_ENDIAN) ?
                Numbers.intBytesBE(value) : Numbers.intBytesLE(value);
    }
}
