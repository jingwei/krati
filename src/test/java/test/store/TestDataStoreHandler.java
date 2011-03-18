package test.store;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;

import junit.framework.TestCase;
import krati.store.DataStoreHandler;
import krati.store.DefaultDataStoreHandler;

/**
 * TestDataStoreHandler
 * 
 * @author jwu
 * 03/18, 2011
 *
 */
public class TestDataStoreHandler extends TestCase {
    static Random rand = new Random();
    
    protected DataStoreHandler createDataStoreHandler() {
        return new DefaultDataStoreHandler();
    }
    
    protected byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        rand.nextBytes(bytes);
        return bytes;
    }
    
    public void testApiBasics() {
        byte[] key1 = randomBytes(32);
        byte[] value1 = randomBytes(1024);
        
        byte[] key2 = randomBytes(64);
        byte[] value2 = randomBytes(1024);
        
        byte[] key3 = randomBytes(128);
        byte[] value3 = randomBytes(1024);
        applyBasicOps(key1, value1, key2, value2, key3, value3);
        
        value1 = randomBytes(0);
        value2 = randomBytes(0);
        value3 = randomBytes(0);
        applyBasicOps(key1, value1, key2, value2, key3, value3);
    }
    
    private void applyBasicOps(byte[] key1, byte[] value1, byte[] key2, byte[] value2, byte[] key3, byte[] value3) {
        DataStoreHandler h = createDataStoreHandler();
        byte[] data;
        
        byte[] data1 = h.assemble(key1, value1);
        byte[] data2 = h.assemble(key1, value1, null);
        assertTrue(Arrays.equals(data1, data2));
        data2 = h.assemble(key1, value1, new byte[0]);
        assertTrue(Arrays.equals(data1, data2));
        
        data = h.assemble(key1, value1);
        assertTrue(Arrays.equals(value1, h.extractByKey(key1, data)));
        assertEquals(1, h.countCollisions(key1, data));
        
        data = h.assemble(key2, value2, data);
        assertTrue(Arrays.equals(value1, h.extractByKey(key1, data)));
        assertTrue(Arrays.equals(value2, h.extractByKey(key2, data)));
        assertEquals(2, h.countCollisions(key1, data));
        assertEquals(2, h.countCollisions(key2, data));
        
        data = h.assemble(key3, value3, data);
        assertTrue(Arrays.equals(value1, h.extractByKey(key1, data)));
        assertTrue(Arrays.equals(value2, h.extractByKey(key2, data)));
        assertTrue(Arrays.equals(value3, h.extractByKey(key3, data)));
        assertEquals(3, h.countCollisions(key1, data));
        assertEquals(3, h.countCollisions(key2, data));
        assertEquals(3, h.countCollisions(key3, data));
        
        int newLength = h.removeByKey(key3, data);
        data = Arrays.copyOf(data, newLength);
        assertTrue(Arrays.equals(value1, h.extractByKey(key1, data)));
        assertTrue(Arrays.equals(value2, h.extractByKey(key2, data)));
        assertEquals(null, h.extractByKey(key3, data));
        assertEquals(2, h.countCollisions(key1, data));
        assertEquals(2, h.countCollisions(key2, data));
        assertEquals(-2, h.countCollisions(key3, data));
        
        newLength = h.removeByKey(key1, data);
        data = Arrays.copyOf(data, newLength);
        assertEquals(null, h.extractByKey(key1, data));
        assertTrue(Arrays.equals(value2, h.extractByKey(key2, data)));
        assertEquals(-1, h.countCollisions(key1, data));
        assertEquals(1, h.countCollisions(key2, data));
        
        newLength = h.removeByKey(key2, data);
        data = Arrays.copyOf(data, newLength);
        assertEquals(null, h.extractByKey(key1, data));
        assertEquals(0, h.countCollisions(key1, data));
        assertEquals(null, h.extractByKey(key2, data));
        assertEquals(0, h.countCollisions(key2, data));
        assertEquals(null, h.extractByKey(key3, data));
        assertEquals(0, h.countCollisions(key3, data));
        
        data1 = h.assemble(key1, value1);
        data1 = h.assemble(key2, value2, data1);
        data1 = h.assemble(key3, value3, data1);
        
        List<byte[]> keys = h.extractKeys(data1);
        assertEquals(3, keys.size());
        List<Entry<byte[], byte[]>> entries = h.extractEntries(data1);
        assertEquals(3, entries.size());
        
        data2 = h.assembleEntries(entries);
        assertTrue(Arrays.equals(data1, data2));
    }
    
    public void testApiNullValues() {
        DataStoreHandler h = createDataStoreHandler();
        byte[] data;
        
        byte[] key = randomBytes(8);
        byte[] value = null;
        
        data = h.assemble(key, value);
        assertEquals(null, data);
        
        byte[] key1 = randomBytes(32);
        byte[] value1 = randomBytes(1024);
        
        byte[] data1 = h.assemble(key1, value1);
        byte[] data2 = h.assemble(key1, value1, null);
        assertTrue(Arrays.equals(data1, data2));
        
        byte[] data3 = h.assemble(key, value, data1);
        assertTrue(Arrays.equals(data1, data3));
        
        assertEquals(null, h.extractByKey(key, data1));
        assertEquals(data1.length, h.removeByKey(key, data1));
    }
}
