package test.store;

import java.io.File;

import krati.core.StoreFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import test.StatsLog;

/**
 * TestIndexedStore using MemorySegment.
 * 
 * @author jwu
 *
 */
public class TestIndexedStore extends EvalDataStore {

    public TestIndexedStore() {
        super(TestIndexedStore.class.getSimpleName());
    }
    
    protected SegmentFactory createIndexSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    protected SegmentFactory createStoreSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        int initialCapacity = (int)(_keyCount * 1.5);
        return StoreFactory.createIndexedDataStore(
                storeDir,
                initialCapacity,
                10000,                       /* batchSize */
                5,                           /* numSyncBatches */
                32,                          /* index segmentFileSizeMB */
                createIndexSegmentFactory(), /* index segmentFactory */
                _segFileSizeMB,              /* store segmentFileSizeMB */
                createStoreSegmentFactory()  /* store segmentFactory */);
    }
    
    public void testIndexedStore() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        cleanTestOutput();
        
        StatsLog.endUnit(unitTestName);
    }
}
