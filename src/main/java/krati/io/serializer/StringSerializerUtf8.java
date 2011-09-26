package krati.io.serializer;

import java.nio.charset.Charset;

import krati.io.SerializationException;
import krati.io.Serializer;

/**
 * StringSerializerUtf8
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class StringSerializerUtf8 implements Serializer<String> {
    private final static Charset UTF8 = Charset.forName("UTF-8");
    
    /**
     * Deserialize a byte array to String using the UTF-8 charset.
     * 
     * @throws NullPointerException if the <tt>raw</tt> is null.
     */
    @Override
    public String deserialize(byte[] raw) throws SerializationException {
        return new String(raw, UTF8);
    }
    
    /**
     * Serialize a String to a byte array using the UTF-8 charset.
     * 
     * @throws NullPointerException if the <tt>str</tt> is null.
     */
    @Override
    public byte[] serialize(String str) throws SerializationException {
        return str.getBytes(UTF8);
    }
}
