package test.core.api;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import test.util.FileUtils;

import junit.framework.TestCase;
import krati.core.array.AddressArray;
import krati.core.array.AddressArrayFactory;

/**
 * AbstractTestDynamicAddressArray
 * 
 * @author jwu
 * 06/21, 2011
 * 
 * <p>
 * 06/23, 2011 - Added testAddressArrayFactory
 */
public abstract class AbstractTestDynamicAddressArray extends TestCase {
    protected final Random _rand = new Random();
    protected AddressArray _array;
    
    protected void setUp() {
        try {
            _array = createAddressArray(getHomeDir());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    protected void tearDown() {
        try {
            FileUtils.deleteDirectory(getHomeDir());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    protected File getHomeDir() {
        return FileUtils.getTestDir(getClass().getSimpleName());
    }
    
    protected int getBatchSize() {
        return 100;
    }
    
    protected int getNumSyncBatches() {
        return 5;
    }
    
    protected abstract AddressArray createAddressArray(File homeDir) throws Exception;
    
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
    private Map<Integer, Long> onArray(AddressArray array, int anyIndex, int numOps, boolean clearAll) throws Exception {
        array.expandCapacity(anyIndex);
        int length = array.length();
        assertTrue(anyIndex < length);
        
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        for(int i = 0; i < numOps; i++) {
            int index = _rand.nextInt(length);
            long value = _rand.nextLong();
            array.set(index, value, System.nanoTime());
            assertEquals(value, array.get(index));
        }
        
        for(Map.Entry<Integer, Long> e : map.entrySet()) {
            int index = e.getKey();
            long value = e.getValue();
            assertEquals(index + "=" + value + "," + array.get(index), value, array.get(index));
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
        
        long hwmMark = _array.getHWMark();
        
        _array.close();
        _array.open();
        
        assertEquals(hwmMark, _array.getLWMark());
        assertEquals(hwmMark, _array.getHWMark());
        
        // Check after re-open
        for(Map.Entry<Integer, Long> e : map.entrySet()) {
            assertEquals(e.getValue().longValue(), _array.get(e.getKey()));
        }
        
        File homeDir = FileUtils.getTestDir(getClass().getSimpleName());
        AddressArray array2 = createAddressArray(homeDir);
        
        assertEquals(hwmMark, array2.getLWMark());
        assertEquals(hwmMark, array2.getHWMark());
        
        // Check newly opened array
        for(Map.Entry<Integer, Long> e : map.entrySet()) {
            assertEquals(e.getValue().longValue(), array2.get(e.getKey()));
        }
    }
    
    public void testWaterMarks() throws Exception {
        int length = _array.length();
        int anyIndex = _rand.nextInt(length << 1);
        boolean clearAll = false;
        
        onArray(_array, anyIndex, _rand.nextInt(_array.length()), clearAll);
        length = _array.length();
        
        onArray(_array, anyIndex, _rand.nextInt(_array.length()), clearAll);
        assertTrue(_array.getLWMark() <= _array.getHWMark());
        _array.persist();
        assertEquals(_array.getLWMark(), _array.getHWMark());
        
        onArray(_array, anyIndex, _rand.nextInt(_array.length()), clearAll);
        assertTrue(_array.getLWMark() <= _array.getHWMark());
        _array.sync();
        assertEquals(_array.getLWMark(), _array.getHWMark());
        
        _array.set(_rand.nextInt(length), _rand.nextLong(), _array.getHWMark() + 1);
        assertTrue(_array.getLWMark() < _array.getHWMark());
        _array.persist();
        assertEquals(_array.getLWMark(), _array.getHWMark());
        
        _array.set(_rand.nextInt(length), _rand.nextLong(), _array.getHWMark() + 1);
        assertTrue(_array.getLWMark() < _array.getHWMark());
        _array.sync();
        assertEquals(_array.getLWMark(), _array.getHWMark());
        
        // Save endOfPeriod larger than hwMark
        long endOfPeriod = _array.getHWMark() + 10;
        _array.saveHWMark(endOfPeriod);
        assertEquals(endOfPeriod, _array.getHWMark());
        
        // Save endOfPeriod smaller than hwMark
        _array.saveHWMark(endOfPeriod - 5);
        assertEquals(endOfPeriod, _array.getHWMark());
        
        // Save endOfPeriod equal to hwMark
        _array.saveHWMark(_array.getHWMark());
        assertEquals(endOfPeriod, _array.getHWMark());
        
        // Reset lwMark and hwMark to smaller value
        endOfPeriod = _array.getLWMark() - 10;
        _array.saveHWMark(endOfPeriod);
        assertEquals(endOfPeriod, _array.getLWMark());
        assertEquals(endOfPeriod, _array.getHWMark());
        
        onArray(_array, anyIndex, _rand.nextInt(getBatchSize()), clearAll);
        _array.sync();
        assertEquals(_array.getLWMark(), _array.getHWMark());
        
        // Test hwMark upon re-open and re-create
        endOfPeriod = _array.getHWMark();
        _array.close();
        _array.open();

        assertEquals(endOfPeriod, _array.getLWMark());
        assertEquals(endOfPeriod, _array.getHWMark());
        
        AddressArray array2 = createAddressArray(getHomeDir());
        assertEquals(_array.getLWMark(), array2.getLWMark());
        assertEquals(_array.getHWMark(), array2.getHWMark());
    }
    
    public void testAddressArrayFactory() throws Exception {
        checkAddressArrayFactory(_array);
        
        for(int i = 0; i < 10; i++) {
            _array.expandCapacity(_array.length() + _rand.nextInt(_array.length()));
            checkAddressArrayFactory(_array);
        }
    }
    
    private void checkAddressArrayFactory(AddressArray array) throws Exception {
        int length = array.length();
        
        AddressArrayFactory factory1 = new AddressArrayFactory(true);
        AddressArrayFactory factory2 = new AddressArrayFactory(false);
        
        AddressArray addrArray1 = factory1.createDynamicAddressArray(
                getHomeDir(), getBatchSize(), getNumSyncBatches());
        
        AddressArray addrArray2 = factory2.createDynamicAddressArray(
                getHomeDir(), getBatchSize(), getNumSyncBatches());
        
        assertEquals(length, addrArray1.length());
        assertEquals(length, addrArray2.length());
        
        addrArray1.close();
        addrArray1.open();
        assertEquals(length, addrArray1.length());
        addrArray1.close();
        
        addrArray2.close();
        addrArray2.open();
        assertEquals(length, addrArray2.length());
        addrArray2.close();
    }
}
