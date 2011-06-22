package test.core.api;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import test.util.FileUtils;

import junit.framework.TestCase;
import krati.array.DynamicArray;
import krati.core.array.AddressArray;

/**
 * AbstractTestDynamicAddressArray
 * 
 * @author jwu
 * 06/21, 2011
 * 
 */
public abstract class AbstractTestDynamicAddressArray<T extends AddressArray & DynamicArray > extends TestCase {
    protected final Random _rand = new Random();
    protected T _array;
    
    protected void setUp() {
        try {
            File homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            _array = createAddressArray(homeDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    protected void tearDown() {
        try {
            File homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            FileUtils.deleteDirectory(homeDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    protected int getBatchSize() {
        return 100;
    }
    
    protected int getNumSyncBatches() {
        return 5;
    }
    
    protected abstract T createAddressArray(File homeDir) throws Exception;
    
    public void testApiBasics() throws Exception {
        int length = _array.length();
        int anyIndex = _rand.nextInt(length);
        boolean clearAll = true;
        
        onArray(_array, anyIndex, _rand.nextInt(100), clearAll);
        
        anyIndex = length + _rand.nextInt(length);
        onArray(_array, anyIndex, _rand.nextInt(100), clearAll);
        assertTrue(length < _array.length());
        
        length = _array.length();
        anyIndex = length + _rand.nextInt(length);
        onArray(_array, anyIndex, _rand.nextInt(100), clearAll);
        assertTrue(length < _array.length());
        
        length = _array.length();
        anyIndex = length + _rand.nextInt(length << 5);
        onArray(_array, anyIndex, _rand.nextInt(100), clearAll);
        assertTrue(length < _array.length());
    }
    
    /**
     * Performs set/get/length/expandCapacity/clear operations on an array having a given index.
     * 
     * @param array    - Address array 
     * @param anyIndex - Index to be contained by array
     * @param numOps   - Number of set operations to perform
     * @param clearAll - Whether to clear the impact of set operations? 
     * @return
     * @throws Exception
     */
    private Map<Integer, Long> onArray(T array, int anyIndex, int numOps, boolean clearAll) throws Exception {
        array.expandCapacity(anyIndex);
        int length = array.length();
        assertTrue(anyIndex < length);
        
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        for(int i = 0; i < numOps; i++) {
            int index = _rand.nextInt(length);
            long value = _rand.nextLong();
            map.put(index, value);
            array.set(index, value, System.currentTimeMillis());
            assertEquals(value, array.get(index));
        }
        
        for(Map.Entry<Integer, Long> e : map.entrySet()) {
            int index = e.getKey();
            long value = e.getValue();
            assertEquals(value, array.get(index));
        }
        
        long[] internalArray = array.getInternalArray();
        assertEquals(length, internalArray.length);
        for(Map.Entry<Integer, Long> e : map.entrySet()) {
            assertEquals(e.getValue().longValue(), internalArray[e.getKey()]);
        }
        
        if(clearAll) {
            array.clear();
            for(Integer index: map.keySet()) {
                assertEquals(0, array.get(index));
            }
        }
        
        return map;
    }
    
    public void testOpenClose() throws Exception {
        int length = _array.length();
        int anyIndex = _rand.nextInt(length << 1);
        boolean clearAll = false;
        
        Map<Integer, Long> map = onArray(_array, anyIndex, _rand.nextInt(_array.length()), clearAll);
        
        // Check before close
        for(Map.Entry<Integer, Long> e : map.entrySet()) {
            assertEquals(e.getValue().longValue(), _array.get(e.getKey()));
        }
        
        _array.close();
        _array.open();
        
        // Check after re-open
        for(Map.Entry<Integer, Long> e : map.entrySet()) {
            assertEquals(e.getValue().longValue(), _array.get(e.getKey()));
        }
        
        File homeDir = FileUtils.getTestDir(getClass().getSimpleName());
        T array2 = createAddressArray(homeDir);
        
        // Check newly opened array
        for(Map.Entry<Integer, Long> e : map.entrySet()) {
            assertEquals(e.getValue().longValue(), array2.get(e.getKey()));
        }
    }
}
