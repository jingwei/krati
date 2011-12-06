package krati.io.serializer;

import krati.io.SerializationException;
import krati.io.Serializer;

/**
 * StringSerializer
 * 
 * @author jwu
 * @since 08/06, 2011
 */
public class StringSerializer implements Serializer<String> {
    
    /**
     * Deserialize a byte array to String using the platform's default charset.
     * 
     * @throws NullPointerException if the <tt>bytes</tt> is null.
     */
    @Override
    public String deserialize(byte[] bytes) throws SerializationException {
        return new String(bytes);
    }
    
    /**
     * Serialize a String to a byte array using the platform's default charset.
     * 
     * @throws NullPointerException if the <tt>str</tt> is null.
     */
    @Override
    public byte[] serialize(String str) throws SerializationException {
        return str.getBytes();
    }
}
