package test.store;

import java.io.File;

import test.StatsLog;

import krati.core.StoreFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataStore;

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
        int initialCapacity = (int)(_keyCount * 1.5);
        return StoreFactory.createDynamicDataStore(storeDir, initialCapacity, _segFileSizeMB, getSegmentFactory());
    }
    
    public void testDynamicStore() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
