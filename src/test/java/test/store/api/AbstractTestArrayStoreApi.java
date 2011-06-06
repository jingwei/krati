package test.store.api;

import java.io.File;
import java.util.Arrays;
import java.util.Random;

import junit.framework.TestCase;
import krati.store.ArrayStore;
import test.util.FileUtils;
import test.util.RandomBytes;

/**
 * AbstractTestArrayStoreApi
 * 
 * @author jwu
 * 06/06, 2011
 * 
 */
public abstract class AbstractTestArrayStoreApi extends TestCase {
    protected File _homeDir;
    protected ArrayStore _store;
    protected final Random _rand = new Random();
    
    @Override
    protected void setUp() {
        try {
            _homeDir = FileUtils.getTestDir(getClass().getSimpleName());
            _store = createStore(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    @Override
    protected void tearDown() {
        try {
            _store.close();
            FileUtils.deleteDirectory(_homeDir);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    protected abstract ArrayStore createStore(File homeDir) throws Exception;
    
    public void testApiBasics() throws Exception {
        int start = _store.getIndexStart();
        int capacity = _store.capacity();
        int index = start + _rand.nextInt(capacity);
        long scn = System.currentTimeMillis();
        byte[] value = RandomBytes.getBytes();
        
        // get/set/delete/clear
        _store.set(index, value, scn++);
        assertTrue(Arrays.equals(value, _store.get(index)));
        
        _store.set(index, null, scn++);
        assertTrue(Arrays.equals(null, _store.get(index)));
        
        _store.set(index, value, scn++);
        assertTrue(Arrays.equals(value, _store.get(index)));
        
        _store.delete(index, scn++);
        assertTrue(Arrays.equals(null, _store.get(index)));
        
        _store.set(index, value, scn++);
        assertTrue(Arrays.equals(value, _store.get(index)));
        
        _store.clear();
        assertTrue(Arrays.equals(null, _store.get(index)));
        
        _store.set(index, value, scn++);
        assertTrue(Arrays.equals(value, _store.get(index)));
        
        // open/close
        assertTrue(_store.isOpen());
        _store.close();
        assertFalse(_store.isOpen());
        _store.open();
        assertTrue(_store.isOpen());
        
        assertTrue(Arrays.equals(value, _store.get(index)));
        assertEquals(_store.getLWMark(), _store.getHWMark());
        
        // persist/sync
        index = start + _rand.nextInt(capacity);
        value = RandomBytes.getBytes();
        _store.set(index, value, scn++);
        assertTrue(_store.getLWMark() < _store.getHWMark());
        
        _store.persist();
        assertEquals(_store.getLWMark(), _store.getHWMark());
        
        index = start + _rand.nextInt(capacity);
        value = RandomBytes.getBytes();
        _store.set(index, value, scn++);
        assertTrue(_store.getLWMark() < _store.getHWMark());
        
        _store.sync();
        assertEquals(_store.getLWMark(), _store.getHWMark());
    }
    
    public void testSetGet() throws Exception {
        int start = _store.getIndexStart();
        int capacity = _store.capacity();
        int index = start + _rand.nextInt(capacity);
        long scn = System.currentTimeMillis();
        byte[] value = RandomBytes.getBytes();
        int offset = 5;
        
        _store.set(index, value, scn++);
        assertTrue(Arrays.equals(value, _store.get(index)));
        
        byte[] dst = new byte[value.length];
        _store.get(index, dst);
        assertTrue(Arrays.equals(value, dst));
        
        dst = new byte[value.length + offset];
        _store.get(index, dst, offset);
        assertTrue(byteArrayEquals(value, 0, dst, offset, value.length));
        
        _store.set(index, null, scn++);
        assertTrue(Arrays.equals(null, _store.get(index)));
        
        if(value.length > offset) {
            _store.set(index, value, offset, value.length - offset, scn++);
            dst = _store.get(index);
            assertTrue(byteArrayEquals(value, offset, dst, 0, dst.length));
            
            dst = new byte[value.length - offset];
            _store.get(index, dst);
            assertTrue(byteArrayEquals(value, offset, dst, 0, dst.length));
            
            dst = new byte[value.length];
            _store.get(index, dst);
            assertTrue(byteArrayEquals(value, offset, dst, 0, value.length - offset));
            _store.get(index, dst, offset);
            assertTrue(byteArrayEquals(value, offset, dst, offset, value.length - offset));
        }
        
        _store.sync();
        assertEquals(_store.getLWMark(), _store.getHWMark());
    }
    
    public void testRadom() throws Exception {
        int cnt = _rand.nextInt(100);
        for(int i = 0; i < cnt; i++) {
            testSetGet();
        }
        testApiBasics();
    }
    
    private boolean byteArrayEquals(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
        for(int i = 0; i < length; i++) {
            if(src[srcOffset + i] != dst[dstOffset + i]) return false;
        }
        return true;
    }
}
