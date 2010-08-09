package test.cds.set;

import java.io.File;

import krati.cds.impl.segment.MemorySegmentFactory;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.store.DynamicDataSet;
import krati.cds.store.DataSet;
import test.StatsLog;

public class TestDynamicDataSet extends EvalDataSet
{
    public TestDynamicDataSet()
    {
        super(TestDynamicDataSet.class.getSimpleName());
    }
    
    @Override
    protected DataSet<byte[]> getDataSet(File storeDir) throws Exception
    {
        return new DynamicDataSet(storeDir, 2 /* initLevel */, _segFileSizeMB, getSegmentFactory());
    }
    
    @Override
    protected SegmentFactory getSegmentFactory()
    {
        return new MemorySegmentFactory();
    }
    
    public void testPerformance() throws Exception
    {
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
