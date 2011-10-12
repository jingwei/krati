package test.store;

import java.io.File;
import java.util.Random;
import java.util.Map.Entry;

import test.util.FileUtils;
import junit.framework.TestCase;
import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.io.serializer.StringSerializerUtf8;
import krati.store.ArrayStore;
import krati.store.SerializableObjectArray;
import krati.util.IndexedIterator;

/**
 * TestSerializableObjectArray
 * 
 * @author jwu
 * @since 10/08, 2011
 */
public class TestSerializableObjectArray extends TestCase {
    protected File _homeDir;
    protected SerializableObjectArray<String> _store;
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
    
    protected int getIndexStart() {
        return _store.getStore().getIndexStart();
    }
    
    protected int getRandomIndex() {
        return getIndexStart() + _rand.nextInt(_store.capacity());
    }
    
    protected SerializableObjectArray<String> createStore(File homeDir) throws Exception {
        StoreConfig config = new StoreConfig(homeDir, 1000);
        ArrayStore arrayStore = StoreFactory.createDynamicArrayStore(config);
        return new SerializableObjectArray<String>(arrayStore, new StringSerializerUtf8());
    }
    
    public void testIterator() throws Exception {
        int randomIndex = getRandomIndex();
        String randomValue = "value " + randomIndex;
        
        for(int i = 0; i < 100; i++) {
            int index = getRandomIndex();
            String value = "here is data  for " + index;
            _store.set(index, value);
        }
        
        _store.set(randomIndex, randomValue);
        
        _store.sync();
        
        // Test keyIterator
        IndexedIterator<Integer> iter1 = _store.keyIterator();
        assertEquals(getIndexStart(), iter1.index());
        assertTrue(iter1.hasNext());
        
        int cnt1 = 0;
        while(iter1.hasNext()) {
            iter1.next();
            cnt1++;
        }
        assertEquals(_store.capacity(), cnt1);
        
        for(int i = 0; i < 100; i++) {
            iter1.reset(getRandomIndex());
        }
        
        try {
            iter1.reset(getIndexStart() - 1);
            assertTrue(false);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        try {
            iter1.reset(getIndexStart() + _store.capacity());
            assertTrue(false);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        iter1.reset(randomIndex);
        assertTrue(iter1.hasNext());
        assertEquals(randomIndex, iter1.index());
        assertEquals(randomIndex, iter1.next().intValue());
        
        // Test iterator
        IndexedIterator<Entry<Integer, String>> iter2 = _store.iterator();
        assertEquals(getIndexStart(), iter2.index());
        assertTrue(iter2.hasNext());
        
        int cnt2 = 0;
        while(iter2.hasNext()) {
            iter2.next();
            cnt2++;
        }
        assertEquals(_store.capacity(), cnt2);
        
        for(int i = 0; i < 100; i++) {
            iter2.reset(getRandomIndex());
        }
        
        try {
            iter2.reset(getIndexStart() - 1);
            assertTrue(false);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        try {
            iter2.reset(getIndexStart() + _store.capacity());
            assertTrue(false);
        } catch(ArrayIndexOutOfBoundsException e) {}
        
        iter2.reset(randomIndex);
        assertTrue(iter2.hasNext());
        assertEquals(randomIndex, iter2.index());
        assertTrue(randomValue.equals(iter2.next().getValue()));
    }
}
