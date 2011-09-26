package test.io.serialization;

import junit.framework.TestCase;
import krati.io.Serializer;
import krati.io.serializer.StringSerializer;
import krati.io.serializer.StringSerializerUtf8;

/**
 * TestSerializer
 * 
 * @author jwu
 * @since 09/25, 2011
 */
public class TestSerializer extends TestCase {
    
    public void testStringSerializer() {
        Serializer<String> serializer = new StringSerializer();
        
        String str1 = TestSerializer.class.getSimpleName();
        String str2 = serializer.deserialize(serializer.serialize(str1));
        assertEquals(str1, str2);
    }
    
    public void testStringSerializerUtf8() {
        Serializer<String> serializer = new StringSerializerUtf8();
        
        String str1 = TestSerializer.class.getSimpleName();
        String str2 = serializer.deserialize(serializer.serialize(str1));
        assertEquals(str1, str2);
    }
}
