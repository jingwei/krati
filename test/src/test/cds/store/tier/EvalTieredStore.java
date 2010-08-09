package test.cds.store.tier;

import java.io.File;

import krati.cds.impl.segment.SegmentFactory;
import krati.cds.impl.store.tier.TieredDataStore;
import krati.cds.store.DataStore;
import test.StatsLog;
import test.cds.store.EvalDataStore;

public abstract class EvalTieredStore extends EvalDataStore
{
    public EvalTieredStore()
    {
        super(EvalTieredStore.class.getName());
    }
    
    protected abstract SegmentFactory getSegmentFactory();
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        return new TieredDataStore(storeDir, _initLevel /* initial level */, _segFileSizeMB, getSegmentFactory());
    }
    
    public void testDynamicStore() throws Exception
    {
        String unitTestName = getClass().getSimpleName() + " with " + getSegmentFactory().getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        cleanTestOutput();
        
        evalPerformance(_numReaders, 1, _runTimeSeconds);
        
        StatsLog.endUnit(unitTestName);
    }
}
