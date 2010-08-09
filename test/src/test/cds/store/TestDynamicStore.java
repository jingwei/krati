package test.cds.store;

import java.io.File;

import test.StatsLog;

import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.store.DynamicDataStore;
import krati.cds.store.DataStore;

/**
 * TestDynamicStore using MemorySegment.
 * 
 * @author jwu
 *
 */
public class TestDynamicStore extends EvalDataStore
{
    public TestDynamicStore()
    {
        super(TestDynamicStore.class.getName());
    }
    
    protected SegmentFactory getSegmentFactory()
    {
        return new krati.cds.impl.segment.MemorySegmentFactory();
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        return new DynamicDataStore(storeDir, _initLevel /* initial level */, _segFileSizeMB, getSegmentFactory());
    }
    
    public void testDynamicStore() throws Exception
    {
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        
        cleanTestOutput();
        StatsLog.endUnit(unitTestName);
    }
}
