package test.store;

import java.io.File;

import krati.core.StoreConfig;
import krati.core.StoreFactory;
import krati.store.DataStore;
import test.StatsLog;

/**
 * TestIndexedStoreSmallIndex
 * 
 * @author jwu
 * @since 08/15, 2012
 */
public class TestIndexedStoreSmallIndex extends EvalDataStore {

    public TestIndexedStoreSmallIndex() {
        super(TestIndexedStore.class.getSimpleName());
    }
    
    public int getInitialCapacity() {
        return _keyCount / 32;
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        StoreConfig config = new StoreConfig(storeDir, getInitialCapacity());
        config.setBatchSize(10000);
        config.setNumSyncBatches(10);
        config.setSegmentFactory(new krati.core.segment.WriteBufferSegmentFactory());
        config.setSegmentFileSizeMB(_segFileSizeMB);
        config.setSegmentCompactFactor(0.67);
        
        // Disable Linear Hashing
        config.setHashLoadFactor(1.0d);
        
        // Set Index Segment Size to 32 MB
        config.setInt(StoreConfig.PARAM_INDEX_SEGMENT_FILE_SIZE_MB, 32);
        
        return StoreFactory.createIndexedDataStore(config);
    }
    
    public void testIndexedStore() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        cleanTestOutput();
        
        StatsLog.endUnit(unitTestName);
    }
}
