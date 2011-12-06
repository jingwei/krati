package test.store.api;

import java.io.File;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataStore;
import krati.store.IndexedDataStore;

/**
 * TestIndexedDataStoreIterator
 * 
 * @author  jwu
 * @since   0.4.2
 * @version 0.4.2
 */
public class TestIndexedDataStoreIterator extends AbstractTestDataStoreIterator {
    
    @Override
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        return new IndexedDataStore(
                homeDir,
                1 << 16, /* initialCapacity */
                100,     /* batchSize */
                5,       /* numSyncBatches */
                8,       /* index segmentFileSizeMB */
                new MemorySegmentFactory(),
                16,      /* store segmentFileSizeMB */
                new MappedSegmentFactory());
    }
}
