package test.store;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

import test.util.FileUtils;
import test.util.RandomBytes;
import junit.framework.TestCase;
import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.Segment;
import krati.core.segment.SegmentFactory;
import krati.store.BytesDB;

/**
 * TestBytesDB
 * 
 * @author jwu
 * 06/01, 2011
 * 
 */
public class TestBytesDB extends TestCase {
    protected BytesDB _bytesDB;
    protected final Random _rand = new Random();
    
    @Override
    protected void setUp() {
        try {
            File homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            _bytesDB = new BytesDB(homeDir, 0, 1000, 5, Segment.minSegmentFileSizeMB, createSegmentFactory());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            if(_bytesDB != null) {
                _bytesDB.close();
                FileUtils.deleteDirectory(_bytesDB.getHomeDir());
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected SegmentFactory createSegmentFactory() {
        return new MemorySegmentFactory();
    }
    
    /**
     * This test does 4 add(s), 4 get(s) and 2 set(s).
     * 
     * @throws Exception
     */
    public void testApiBasics() throws Exception {
        long scn = System.currentTimeMillis();
        
        byte[] bytes1 = RandomBytes.getBytes();
        byte[] bytes2 = RandomBytes.getBytes();
        
        // Add the first bytes
        int index1 = _bytesDB.add(bytes1, scn++);
        assertTrue(index1 >= 0);
        
        byte[] bytesRead1 = _bytesDB.get(index1);
        assertTrue(Arrays.equals(bytes1, bytesRead1));
        
        // Add the second bytes
        int index2 = _bytesDB.add(bytes2, scn++);
        assertTrue(index2 >= 0);
        
        byte[] bytesRead2 = _bytesDB.get(index2);
        assertTrue(Arrays.equals(bytes2, bytesRead2));
        
        // Reset bytes
        byte[] bytes = RandomBytes.getBytes();
        byte[] bytesRead;
        
        _bytesDB.set(index1, bytes, scn++);
        bytesRead = _bytesDB.get(index1);
        assertTrue(Arrays.equals(bytes, bytesRead));
        
        _bytesDB.set(index2, bytes, scn++);
        bytesRead = _bytesDB.get(index2);
        assertTrue(Arrays.equals(bytes, bytesRead));
        
        int index3 = _bytesDB.add(bytes, scn++);
        assertTrue(index1 != index3 && index2 != index3);
        
        int index4 = _bytesDB.add(bytes, scn++);
        assertTrue(index1 != index4 && index2 != index4 && index3 != index4);
    }
    
    public void testCapacity() throws Exception {
        testApiBasics();
        
        int capacity = _bytesDB.capacity();
        for(int i = 0, cnt = capacity / 4 ; i < cnt; i++) {
            testApiBasics();
        }
        
        int newCapacity = _bytesDB.capacity();
        assertTrue(capacity < newCapacity);
        
        _bytesDB.sync();
        assertEquals(_bytesDB.getHWMark(), _bytesDB.getLWMark());
    }
    
    public void testPersistable() throws Exception {
        assertEquals(0, _bytesDB.getLWMark());
        assertEquals(0, _bytesDB.getHWMark());
        
        testApiBasics();
        
        _bytesDB.persist();
        assertEquals(_bytesDB.getHWMark(), _bytesDB.getLWMark());
        
        testApiBasics();
        testApiBasics();
        testApiBasics();
        
        _bytesDB.sync();
        assertEquals(_bytesDB.getHWMark(), _bytesDB.getLWMark());
    }
    
    public void testCloseable() throws Exception {
        assertTrue(_bytesDB.isOpen());
        
        byte[] bytes = RandomBytes.getBytes();
        int index = _bytesDB.add(bytes, System.currentTimeMillis());
        assertTrue(Arrays.equals(bytes, _bytesDB.get(index)));
        
        testApiBasics();
        assertTrue(_bytesDB.isOpen());
        
        _bytesDB.close();
        assertFalse(_bytesDB.isOpen());
        
        _bytesDB.open();
        assertTrue(_bytesDB.isOpen());
        
        testApiBasics();
        assertTrue(_bytesDB.isOpen());
        testApiBasics();
        assertTrue(_bytesDB.isOpen());
        
        assertTrue(Arrays.equals(bytes, _bytesDB.get(index)));
        
        _bytesDB.close();
        assertFalse(_bytesDB.isOpen());
        
        _bytesDB.open();
        assertTrue(_bytesDB.isOpen());
        
        assertTrue(Arrays.equals(bytes, _bytesDB.get(index)));
    }
    
    public void testCornerCases() throws Exception {
        long scn = System.currentTimeMillis();
        byte[] bytes = null;
        int index = 0;
        
        index = _bytesDB.add(bytes, scn++);
        assertEquals(null, _bytesDB.get(index));
        assertTrue(Arrays.equals(null, _bytesDB.get(index)));
        
        bytes = new byte[0];
        index = _bytesDB.add(bytes, scn++);
        assertTrue(Arrays.equals(bytes, _bytesDB.get(index)));
        
        _bytesDB.set(index, null, scn++);
        assertEquals(null, _bytesDB.get(index));
    }
    
    public void testClear() throws Exception {
        testApiBasics();
        
        byte[] bytes = RandomBytes.getBytes();
        int index = _bytesDB.add(bytes, System.currentTimeMillis());
        assertTrue(Arrays.equals(bytes, _bytesDB.get(index)));
        
        /* ***************************************************
         * Method clear is NOT effective if BytesDB is closed.
         */
        
        _bytesDB.close();
        assertFalse(_bytesDB.isOpen());
        
        _bytesDB.clear();
        
        _bytesDB.open();
        assertTrue(_bytesDB.isOpen());
        
        assertTrue(Arrays.equals(bytes, _bytesDB.get(index)));

        /* ***************************************************
         * Method clear is effective only if BytesDB is open.
         */
        
        _bytesDB.clear();
        assertEquals(null, _bytesDB.get(index));
        
        _bytesDB.close();
        assertFalse(_bytesDB.isOpen());
        
        _bytesDB.open();
        assertTrue(_bytesDB.isOpen());
        
        assertEquals(null, _bytesDB.get(index));
    }
}
