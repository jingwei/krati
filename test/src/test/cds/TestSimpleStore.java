package test.cds;

import java.io.File;

import test.StatsLog;

import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.store.SimpleDataStore;
import krati.cds.store.DataStore;

/**
 * TestSimpleStore using MemorySegment.
 * 
 * @author jwu
 *
 */
public class TestSimpleStore extends EvalDataStore
{
    public TestSimpleStore()
    {
        super(TestSimpleStore.class.getName());
    }
    
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MemorySegmentFactory();
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        int capacity = (int)(_keyCount * 1.25);
        return new SimpleDataStore(storeDir,
                                   capacity, /* capacity */
                                   10000,    /* entrySize */
                                   5,        /* maxEntries */
                                   _segFileSizeMB,
                                   getSegmentFactory());
    }
    
    public void testSimpleStore() throws Exception
    {
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        new TestSimpleStore().evalPerformance(4, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
