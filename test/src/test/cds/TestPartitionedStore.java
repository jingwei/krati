package test.cds;

import java.io.File;

import test.StatsLog;

import krati.cds.impl.store.PartitionedDataStore;
import krati.cds.store.DataStore;

/**
 * Test Partitioned DataStore.
 * 
 * @author jwu
 *
 */
public class TestPartitionedStore extends EvalDataStore
{
    public TestPartitionedStore()
    {
        super(TestPartitionedStore.class.getSimpleName());
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception
    {
        int partitionCapacity = _idCount/5;
        return new PartitionedDataStore(storeDir, 5, partitionCapacity);
    }
    
    public void testPartitionedStore() throws Exception
    {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        new TestPartitionedStore().evalPerformance(4, 2, _runTimeSeconds);
        cleanTestOutput();
        
        StatsLog.endUnit(unitTestName);
    }
}
