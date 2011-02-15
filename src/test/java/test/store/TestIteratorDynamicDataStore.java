package test.store;

import java.io.File;

import krati.store.DataStore;
import krati.store.DynamicDataStore;

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
        return new DynamicDataStore(storeDir, _initLevel, 10000, 5, _segFileSizeMB, createSegmentFactory());
    }
}
