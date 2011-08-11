package test.store.api;

import java.io.File;

import krati.core.segment.MappedSegmentFactory;
import krati.store.DataStore;
import krati.store.StaticDataStore;

/**
 * TestStaticDataStoreIterator
 * 
 * @author  jwu
 * @since   0.4.2
 * @version 0.4.2
 */
public class TestStaticDataStoreIterator extends AbstractTestDataStoreIterator {
    
    @Override
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        return new StaticDataStore(
                homeDir,
                10000, /* capacity */
                100,   /* batchSize */
                5,     /* numSyncBatches */
                32,    /* segmentFileSizeMB */
                new MappedSegmentFactory());
    }
}
