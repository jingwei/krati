package test.store.api;

import java.io.File;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.MemorySegmentFactory;
import krati.store.DataStore;
import krati.store.IndexedDataStore;

/**
 * TestIndexedDataStoreApi
 * 
 * @author jwu
 * 06/05, 2011
 * 
 */
public class TestIndexedDataStoreApi extends AbstractTestDataStoreApi {

    @Override
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        return new IndexedDataStore(
                homeDir,
                100,   /* batchSize */
                5,     /* numSyncBatches */
                1,     /* index initLevel */
                8,     /* index segmentFileSizeMB */
                new MemorySegmentFactory(),
                1,     /* store initLevel */
                16,    /* store segmentFileSizeMB */
                new MappedSegmentFactory());
    }
}
