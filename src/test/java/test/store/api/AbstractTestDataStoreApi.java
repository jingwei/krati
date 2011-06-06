package test.store.api;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import test.util.FileUtils;
import test.util.RandomBytes;

import junit.framework.TestCase;
import krati.store.DataStore;

/**
 * TestDataStoreApi
 * 
 * @author jwu
 * 06/04, 2011
 * 
 */
public abstract class AbstractTestDataStoreApi extends TestCase {
    protected File _homeDir;
    protected DataStore<byte[], byte[]> _store;
    
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
    
    protected abstract DataStore<byte[], byte[]> createStore(File homeDir) throws Exception;
    
    public void testApiBasics() throws Exception {
        byte[] key = RandomBytes.getBytes(16);
        byte[] value = RandomBytes.getBytes();
        
        _store.put(key, value);
        assertTrue(Arrays.equals(value, _store.get(key)));
        
        _store.put(key, null);
        assertTrue(Arrays.equals(null, _store.get(key)));
        
        _store.put(key, value);
        assertTrue(Arrays.equals(value, _store.get(key)));
        
        _store.delete(key);
        assertTrue(Arrays.equals(null, _store.get(key)));
        
        _store.put(key, value);
        assertTrue(Arrays.equals(value, _store.get(key)));
        
        _store.clear();
        assertTrue(Arrays.equals(null, _store.get(key)));
        
        _store.put(key, value);
        assertTrue(Arrays.equals(value, _store.get(key)));
        
        assertTrue(_store.isOpen());
        _store.close();
        assertFalse(_store.isOpen());
        _store.open();
        assertTrue(_store.isOpen());
        
        assertTrue(Arrays.equals(value, _store.get(key)));
    }
    
    public void testKeyIterator() throws Exception {
        Iterator<byte[]> keyIter;
        
        _store.clear();
        keyIter = _store.keyIterator();
        assertFalse(keyIter.hasNext());
        
        // Generate keys
        HashSet<String> keySet = new HashSet<String>(199);
        while(keySet.size() < 100) {
            keySet.add(new String(RandomBytes.getBytes(16)));
        }
        assertEquals(100, keySet.size());
        
        // Populate store
        for(String key : keySet) {
            byte[] value = RandomBytes.getBytes();
            _store.put(key.getBytes(), value);
        }
        
        // Check keys
        keyIter = _store.keyIterator();
        HashSet<String> keySet2 = new HashSet<String>(199);
        while(keyIter.hasNext()) {
            keySet2.add(new String(keyIter.next()));
        }
        
        assertEquals(keySet.size(), keySet2.size());
        
        keySet2.removeAll(keySet);
        assertEquals(0, keySet2.size());
        
        // Re-open store and check keys
        _store.close();
        _store.open();
        
        keySet2.clear();
        keyIter = _store.keyIterator();
        while(keyIter.hasNext()) {
            keySet2.add(new String(keyIter.next()));
        }
        
        assertEquals(keySet.size(), keySet2.size());
        
        keySet2.removeAll(keySet);
        assertEquals(0, keySet2.size());
        
        // Clear store and check keys
        _store.clear();
        keyIter = _store.keyIterator();
        assertFalse(keyIter.hasNext());
        
        // Re-open store and check keys
        _store.close();
        _store.open();
        keyIter = _store.keyIterator();
        assertFalse(keyIter.hasNext());
    }
    
    public void testIterator() throws Exception {
        Iterator<Entry<byte[], byte[]>> iter;
        
        _store.clear();
        iter = _store.iterator();
        assertFalse(iter.hasNext());
        
        // Generate keys
        HashSet<String> keySet = new HashSet<String>(199);
        while(keySet.size() < 100) {
            keySet.add(new String(RandomBytes.getBytes(16)));
        }
        assertEquals(100, keySet.size());
        
        HashMap<String, byte[]> map = new HashMap<String, byte[]>();
        
        // Populate store
        for(String key : keySet) {
            byte[] value = RandomBytes.getBytes();
            map.put(new String(key), value);
            _store.put(key.getBytes(), value);
        }
        
        // Check key-value pairs
        iter = _store.iterator();
        HashSet<String> keySet2 = new HashSet<String>(199);
        while(iter.hasNext()) {
            Entry<byte[], byte[]> e = iter.next();
            assertTrue(Arrays.equals(map.get(new String(e.getKey())), e.getValue()));
            keySet2.add(new String(e.getKey()));
        }
        
        assertEquals(keySet.size(), keySet2.size());
        
        keySet2.removeAll(keySet);
        assertEquals(0, keySet2.size());
        
        // Re-open store and check key-value pairs
        _store.close();
        _store.open();
        
        keySet2.clear();
        iter = _store.iterator();
        while(iter.hasNext()) {
            Entry<byte[], byte[]> e = iter.next();
            assertTrue(Arrays.equals(map.get(new String(e.getKey())), e.getValue()));
            keySet2.add(new String(e.getKey()));
        }
        
        assertEquals(keySet.size(), keySet2.size());

        keySet2.removeAll(keySet);
        assertEquals(0, keySet2.size());
        
        // Clear store and check key-value pairs
        _store.clear();
        iter = _store.iterator();
        assertFalse(iter.hasNext());
        
        // Re-open store and check key-value pairs
        _store.close();
        _store.open();
        iter = _store.iterator();
        assertFalse(iter.hasNext());
    }
}
