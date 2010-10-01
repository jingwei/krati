package test.store;

import java.io.File;

import krati.store.DataStore;
import krati.store.IndexedDataStore;

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
        return new IndexedDataStore(
                storeDir,
                10000,
                5,
                _initLevel,
                32,
                createSegmentFactory(),/* index segment factory */
                _initLevel,
                _segFileSizeMB,
                createSegmentFactory() /* store segment factory */);
    }
}
