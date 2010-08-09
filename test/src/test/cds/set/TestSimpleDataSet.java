package test.cds.set;

import java.io.File;

import test.StatsLog;

import krati.cds.impl.segment.MemorySegmentFactory;
import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.store.SimpleDataSet;
import krati.cds.store.DataSet;

public class TestSimpleDataSet extends EvalDataSet
{
    public TestSimpleDataSet()
    {
        super(TestSimpleDataSet.class.getSimpleName());
    }
    
    @Override
    protected DataSet<byte[]> getDataSet(File storeDir) throws Exception
    {
        int capacity = (int)(_keyCount * 1.5);
        return new SimpleDataSet(storeDir,
                                 capacity, /* capacity */
                                 10000,    /* entrySize */
                                 5,        /* maxEntries */
                                 _segFileSizeMB,
                                 getSegmentFactory());
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
