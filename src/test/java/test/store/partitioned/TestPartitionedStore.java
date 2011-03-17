package test.store.partitioned;

import java.io.File;

import test.StatsLog;
import test.store.EvalDataStore;

import krati.store.DataStore;

/**
 * Test Partitioned DataStore.
 * 
 * @author jwu
 *
 */
public class TestPartitionedStore extends EvalDataStore {
    
    public TestPartitionedStore() {
        super(TestPartitionedStore.class.getSimpleName());
    }
    
    @Override
    protected DataStore<byte[], byte[]> getDataStore(File storeDir) throws Exception {
        int partitionCapacity = _idCount/5;
        return new PartitionedDataStore(storeDir, 5, partitionCapacity,
                                        new krati.core.segment.MappedSegmentFactory(),
                                        _segFileSizeMB);
    }
    
    public void testPartitionedStore() throws Exception {
        String unitTestName = getClass().getSimpleName(); 
        StatsLog.beginUnit(unitTestName);
        
        evalPerformance(_numReaders, 2, _runTimeSeconds);
        cleanTestOutput();
        
        StatsLog.endUnit(unitTestName);
    }
}
