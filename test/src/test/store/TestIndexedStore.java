package test.store;

import java.io.File;

import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.IndexedDataStore;
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
        return new IndexedDataStore(
                storeDir,
                10000,
                5,
                _initLevel,
                32,
                createIndexSegmentFactory(),
                _initLevel,
                _segFileSizeMB,
                createStoreSegmentFactory());
    }
    
    public void testIndexedStore() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        cleanTestOutput();
        
        StatsLog.endUnit(unitTestName);
    }
}
