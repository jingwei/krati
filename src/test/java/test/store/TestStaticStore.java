package test.store;

import java.io.File;

import test.StatsLog;

import krati.core.segment.SegmentFactory;
import krati.store.DataStore;
import krati.store.StaticDataStore;

/**
 * TestSimpleStore using MemorySegment.
 * 
 * @author jwu
 *
 */
public class TestStaticStore extends EvalDataStore {
    
    public TestStaticStore() {
        super(TestStaticStore.class.getName());
    }
    
    protected SegmentFactory getSegmentFactory() {
        return new krati.core.segment.MemorySegmentFactory();
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        int capacity = (int)(_keyCount * 1.5);
        return new StaticDataStore(storeDir,
                                   capacity, /* capacity */
                                   10000,    /* entrySize */
                                   5,        /* maxEntries */
                                   _segFileSizeMB,
                                   getSegmentFactory());
    }
    
    public void testSimpleStore() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
