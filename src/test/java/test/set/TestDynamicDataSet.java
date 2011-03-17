package test.set;

import java.io.File;

import krati.core.segment.MemorySegmentFactory;
import krati.core.segment.SegmentFactory;
import krati.store.DataSet;
import krati.store.DynamicDataSet;
import test.StatsLog;

/**
 * TestDynamicDataSet
 * 
 * @author jwu
 * 
 */
public class TestDynamicDataSet extends EvalDataSet {
    
    public TestDynamicDataSet() {
        super(TestDynamicDataSet.class.getSimpleName());
    }
    
    @Override
    protected DataSet<byte[]> getDataSet(File storeDir) throws Exception {
        return new DynamicDataSet(storeDir, 2 /* initLevel */, _segFileSizeMB, getSegmentFactory());
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
