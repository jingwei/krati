package test.store;

import java.io.File;

import krati.store.DataStore;
import krati.store.StaticDataStore;

/**
 * TestIteratorStaticDataStore
 * 
 * @author jwu
 * Sep 30, 2010
 */
public class TestIteratorStaticDataStore  extends EvalDataStoreIterator {

    public TestIteratorStaticDataStore() throws Exception {
        super(TestIteratorStaticDataStore.class.getSimpleName());
    }
    
    protected DataStore<byte[], byte[]> createDataStore(File storeDir) throws Exception {
        int capacity = (int)(_keyCount * 1.5);
        return new StaticDataStore(storeDir,
                                   capacity, /* capacity */
                                   10000,    /* batchSize */
                                   5,        /* numSyncBatches */
                                   _segFileSizeMB,
                                   createSegmentFactory());
    }
}
