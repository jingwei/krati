package test.io.serialization;

import java.util.Arrays;

import junit.framework.TestCase;
import krati.io.Serializer;

/**
 * AbstractTestSerializer
 * 
 * @author jwu
 * 07/18, 2011
 * 
 * @param <T> Object to serialize
 */
public abstract class AbstractTestSerializer<T> extends TestCase {
    
    protected abstract T createObject();
    
    protected abstract Serializer<T> createSerializer();
    
    public void testApiBasics() {
        Serializer<T> serializer = createSerializer();
        
        T object1 = createObject();
        byte[] bytes1 = serializer.serialize(object1);
        
        T object2 = serializer.deserialize(bytes1);
        byte[] bytes2 = serializer.serialize(object2);
        
        assertTrue(Arrays.equals(bytes1, bytes2));
    }
}
