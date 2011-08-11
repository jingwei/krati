package test.store.api;

import java.io.File;

import krati.core.segment.MappedSegmentFactory;
import krati.store.DataStore;
import krati.store.DynamicDataStore;

/**
 * TestDynamicDataStoreIterator
 * 
 * @author  jwu
 * @since   0.4.2
 * @version 0.4.2
 */
public class TestDynamicDataStoreIterator extends AbstractTestDataStoreIterator {
    
    @Override
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        return new DynamicDataStore(
                homeDir,
                1,     /* initLevel */
                100,   /* batchSize */
                5,     /* numSyncBatches */
                32,    /* segmentFileSizeMB */
                new MappedSegmentFactory());
    }
}
