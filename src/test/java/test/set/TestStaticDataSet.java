package test.set;

import java.io.File;

import test.StatsLog;

import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataSet;
import krati.store.StaticDataSet;

/**
 * TestStaticDataSet
 * 
 * @author jwu
 * 
 */
public class TestStaticDataSet extends EvalDataSet {
    
    public TestStaticDataSet() {
        super(TestStaticDataSet.class.getSimpleName());
    }
    
    @Override
    protected DataSet<byte[]> getDataSet(File storeDir) throws Exception {
        int capacity = (int)(_keyCount * 1.5);
        return new StaticDataSet(storeDir,
                                 capacity, /* capacity */
                                 10000,    /* entrySize */
                                 5,        /* maxEntries */
                                 _segFileSizeMB,
                                 getSegmentFactory());
    }
    
    @Override
    protected SegmentFactory getSegmentFactory() {
        return new MemorySegmentFactory();
    }
    
    public void testPerformance() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
