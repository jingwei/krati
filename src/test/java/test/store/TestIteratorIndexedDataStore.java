package test.store;

import java.io.File;

import krati.core.StoreFactory;
import krati.store.DataStore;

/**
 * TestIteratorIndexedDataStore
 * 
 * @author jwu
 * Sep 30, 2010
 */
public class TestIteratorIndexedDataStore extends EvalDataStoreIterator {

    public TestIteratorIndexedDataStore() throws Exception {
        super(TestIteratorIndexedDataStore.class.getSimpleName());
    }
    
    protected DataStore<byte[], byte[]> createDataStore(File storeDir) throws Exception {
        int initialCapacity = (int)(_keyCount * 1.5);
        return StoreFactory.createIndexedDataStore(
                storeDir,
                initialCapacity,
                10000,                 /* batchSize */
                5,                     /* numSyncBatches */
                32,                    /* index segmentFileSizeMB */
                createSegmentFactory(),/* index segmentFactory */
                _segFileSizeMB,        /* store segmentFileSizeMB */
                createSegmentFactory() /* store segmentFactory */);
        
    }
}
