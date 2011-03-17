package test.store;

import java.io.File;

import test.StatsLog;

import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.DynamicDataStore;

/**
 * TestDynamicStore using MemorySegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStore extends EvalDataStore {
    
    public TestDynamicStore() {
        super(TestDynamicStore.class.getName());
    }
    
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        return new DynamicDataStore(storeDir, _initLevel /* initial level */, _segFileSizeMB, getSegmentFactory());
    }
    
    public void testDynamicStore() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
