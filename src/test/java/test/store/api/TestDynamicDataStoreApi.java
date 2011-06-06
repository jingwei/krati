package test.store.api;

import java.io.File;

import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.Segment;
import krati.store.DataStore;
import krati.store.DynamicDataStore;

/**
 * TestDynamicDataStoreApi
 * 
 * @author jwu
 * 06/05, 2011
 * 
 */
public class TestDynamicDataStoreApi extends AbstractTestDataStoreApi {

    @Override
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        return new DynamicDataStore(
                homeDir,
                1,     /* initLevel */
                100,   /* batchSize */
                5,     /* numSyncBatches */
                Segment.defaultSegmentFileSizeMB,
                new MappedSegmentFactory());
    }
}
