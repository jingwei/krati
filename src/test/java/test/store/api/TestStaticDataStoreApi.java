package test.store.api;

import java.io.File;


import krati.core.segment.MappedSegmentFactory;
import krati.core.segment.Segment;
import krati.store.DataStore;
import krati.store.StaticDataStore;

/**
 * TestStaticDataStoreApi
 * 
 * @author jwu
 * 06/05, 2011
 * 
 */
public class TestStaticDataStoreApi extends AbstractTestDataStoreApi {

    @Override
    protected DataStore<byte[], byte[]> createStore(File homeDir) throws Exception {
        return new StaticDataStore(
                homeDir,
                10000, /* capacity */
                100,   /* batchSize */
                5,     /* numSyncBatches */
                Segment.defaultSegmentFileSizeMB,
                new MappedSegmentFactory());
    }
}
