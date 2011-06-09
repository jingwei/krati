package test.core.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;
import krati.core.array.basic.MemoryShortArray;

/**
 * TestMemoryShortArray
 * 
 * @author jwu
 * 06/09, 2011
 * 
 */
public class TestMemoryShortArray extends TestCase {
    final Random _rand = new Random();
    
    public void testApiBasics() {
        MemoryShortArray array = new MemoryShortArray();
        int length = array.length();
        int anyIndex = _rand.nextInt(length);
        onArray(array, anyIndex);
        
        anyIndex = length + _rand.nextInt(length);
        onArray(array, anyIndex);
        assertTrue(length < array.length());
        
        length = array.length();
        anyIndex = length + _rand.nextInt(length);
        onArray(array, anyIndex);
        assertTrue(length < array.length());
        
        length = array.length();
        anyIndex = length + _rand.nextInt(length << 5);
        onArray(array, anyIndex);
        assertTrue(length < array.length());
    }
    
    private void onArray(MemoryShortArray array, int anyIndex) {
        array.expandCapacity(anyIndex);
        int length = array.length();
        assertTrue(anyIndex < length);
        
        Map<Integer, Short> map = new HashMap<Integer, Short>();
        for(int i = 0; i < 10; i++) {
            int index = _rand.nextInt(length);
            short value = (short)_rand.nextInt();
            map.put(index, value);
            array.set(index, value);
            assertEquals(value, array.get(index));
        }
        
        short[] internalArray = array.getInternalArray();
        assertEquals(length, internalArray.length);
        for(Map.Entry<Integer, Short> e : map.entrySet()) {
            assertEquals(e.getValue().longValue(), internalArray[e.getKey()]);
        }
        
        array.clear();
        for(Integer index: map.keySet()) {
            assertEquals(0, array.get(index));
        }
    }
    
    public void testMaxLength() {
        MemoryShortArray array = new MemoryShortArray();
        int index = Integer.MAX_VALUE - 1;
        short value = (short)10;
        array.set(index, value);
        assertEquals(value, array.get(index));
        assertEquals(Integer.MAX_VALUE, array.length());
        
        /**
         * The index can go to Integer.MAX_VALUE even array length is Integer.MAX_VALUE
         */
        index = Integer.MAX_VALUE;
        array.set(index, value);
        assertEquals(value, array.get(index));
        assertEquals(Integer.MAX_VALUE, array.length());
    }
}
