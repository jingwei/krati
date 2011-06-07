package test.set.api;

import java.io.File;
import java.util.Random;

import junit.framework.TestCase;
import krati.store.DataSet;
import test.util.FileUtils;
import test.util.RandomBytes;

/**
 * AbstractTestDataSetApi
 * 
 * @author jwu
 * 06/06, 2011
 * 
 */
public abstract class AbstractTestDataSetApi extends TestCase {
    protected File _homeDir;
    protected DataSet<byte[]> _store;
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
    
    protected abstract DataSet<byte[]> createStore(File homeDir) throws Exception;
    
    public void testApiBasics() throws Exception {
        byte[] value;
        
        // random value
        value = RandomBytes.getBytes();
        assertEquals(true, _store.isOpen());
        assertEquals(true, _store.add(value));
        assertEquals(true, _store.has(value));
        assertEquals(true, _store.has(value));
        assertEquals(true, _store.delete(value));
        assertEquals(false, _store.has(value));
        
        // random value
        value = RandomBytes.getBytes();
        assertEquals(true, _store.isOpen());
        assertEquals(true, _store.add(value));
        assertEquals(true, _store.has(value));
        _store.close();
        _store.open();
        assertEquals(true, _store.has(value));
        assertEquals(true, _store.delete(value));
        assertEquals(false, _store.has(value));
        
        _store.close();
        _store.open();
        assertEquals(false, _store.has(value));
        
        // empty value
        value = new byte[0];
        assertEquals(true, _store.isOpen());
        assertEquals(true, _store.add(value));
        assertEquals(true, _store.has(value));
        assertEquals(true, _store.delete(value));
        assertEquals(false, _store.has(value));
        
        _store.close();
        _store.open();
        assertEquals(false, _store.has(value));
        
        // null value
        assertEquals(false, _store.add(null));
        assertEquals(false, _store.has(null));
        assertEquals(false, _store.delete(null));
        
        _store.sync();
    }
    
    public void testClear() throws Exception {
        byte[] value;
        
        value = RandomBytes.getBytes();
        assertEquals(true, _store.isOpen());
        assertEquals(true, _store.add(value));
        assertEquals(true, _store.has(value));
        
        _store.close();
        _store.open();
        assertEquals(true, _store.has(value));
        
        _store.clear();
        assertEquals(false, _store.has(value));
        
        _store.close();
        _store.open();
        assertEquals(false, _store.has(value));
    }
}
