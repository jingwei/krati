package test.store;

import java.io.File;

import krati.core.StoreFactory;
import krati.store.DataStore;

/**
 * TestIteratorDynamicDataStore
 * 
 * @author jwu
 * Sep 30, 2010
 */
public class TestIteratorDynamicDataStore extends EvalDataStoreIterator {

    public TestIteratorDynamicDataStore() throws Exception {
        super(TestIteratorDynamicDataStore.class.getSimpleName());
    }
    
    protected DataStore<byte[], byte[]> createDataStore(File storeDir) throws Exception {
        int initialCapacity = (int)(_keyCount * 1.5);
        return StoreFactory.createDynamicDataStore(storeDir, initialCapacity, 10000, 5, _segFileSizeMB, createSegmentFactory());
    }
}
